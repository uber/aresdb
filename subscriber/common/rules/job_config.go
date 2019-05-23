//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rules

import (
	"encoding/json"
	"fmt"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/controller/models"
	"io/ioutil"
	"os"
	"strings"

	"github.com/getlantern/deepcopy"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/subscriber/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Module configures JobConfigs.
var Module = fx.Options(
	fx.Provide(
		NewJobConfigs,
	),
)

// Params defines the base objects for jobConfigs.
type Params struct {
	fx.In

	ServiceConfig config.ServiceConfig
}

// Result defines the objects that the rules module provides.
type Result struct {
	fx.Out

	JobConfigs JobConfigs
}

// JobConfigs contains configuration and information for all jobs and therir destination ares clusters.
type JobConfigs map[string]JobAresConfig

// JobAresConfig contains configuration and information for each Ares cluster and job configuration.
type JobAresConfig map[string]*JobConfig

// JobConfig wraps job config controller
type JobConfig struct {
	models.JobConfig
	// NumShards is the number of shards defined in this aresCluster
	NumShards uint32 `json:"numShards" yaml:"numShards"`

	// maps from column name to columnID for convenience
	columnDict      map[string]int
	destinations    map[string]*DestinationConfig
	transformations map[string]*TransformationConfig
	primaryKeys     map[string]int
	primaryKeyBytes int
}

// DestinationConfig defines the configuration needed to save data in ares
type DestinationConfig struct {
	// Table is ares table
	Table string `json:"table" yaml:"table"`
	// Column is ares table column name
	Column string `json:"column" yaml:"column"`
	// UpdateMode is column's upsert mode
	UpdateMode memCom.ColumnUpdateMode `json:"update_mode,omitempty" yaml:"update_mode,omitempty"`
}

// TransformationConfig defiines the configuration needed to generate a specific transformation
type TransformationConfig struct {
	// Type of transformationConfig to apply for the column,
	// like timestamp, uuid, uuid_hll etc
	Type string `json:"type" yaml:"type"`
	// Source is the field name to read the value from
	Source string `json:"source" yaml:"source"`
	// Default value to use if value is empty
	Default string `json:"default" yaml:"default"`
	// Context help complex transformations to define information
	// needed to parse the values
	Context map[string]string
}

// Assignment defines the job assignment of the ares-subscriber
type Assignment struct {
	// Subscriber is ares-subscriber instance name
	Subscriber string `json:"subscriber"`
	// Jobs is a list of jobConfigs for the ares-subscriber instance
	Jobs []*JobConfig `json:"jobs"`
	// AresClusters is a table of aresClusters for the ares-subscriber instance
	AresClusters map[string]config.SinkConfig `json:"instances"`
}

// NewJobConfigs creates JobConfigs object
func NewJobConfigs(params Params) (Result, error) {
	jobConfigs := make(JobConfigs)
	err := AddLocalJobConfig(params.ServiceConfig, jobConfigs)

	return Result{
		JobConfigs: jobConfigs,
	}, err

}

// NewAssignmentFromController parse controller assignment and create Assignment rule
func NewAssignmentFromController(from *models.IngestionAssignment) (*Assignment, error) {
	assignment := &Assignment{
		Subscriber: from.Subscriber,
		AresClusters: make(map[string]config.SinkConfig),
	}
	for _, job := range from.Jobs {
		jobConfig := &JobConfig{
			JobConfig: job,
		}
		err := jobConfig.PopulateAresTableConfig()
		if err != nil {
			return nil, err
		}
		assignment.Jobs = append(assignment.Jobs, jobConfig)
	}

	for instanceName, instance := range from.Instances {
		sinkConfig := config.SinkConfig{
			AresDBConnectorConfig: client.ConnectorConfig{
				Address: instance.Address,
			},
		}
		assignment.AresClusters[instanceName] = sinkConfig
	}

	return assignment, nil
}

// GetDestinations returns a job's destination definition
func (j *JobConfig) GetDestinations() map[string]*DestinationConfig {
	return j.destinations
}

// GetTranformations returns a job's tranformation definition
func (j *JobConfig) GetTranformations() map[string]*TransformationConfig {
	return j.transformations
}

// GetPrimaryKeys returns a job's primaryKeys definition
func (j *JobConfig) GetPrimaryKeys() map[string]int {
	return j.primaryKeys
}

// SetPrimaryKeyBytes sets the number of bytes needed by primaryKey
func (j *JobConfig) SetPrimaryKeyBytes(primaryKeyBytes int) {
	j.primaryKeyBytes = primaryKeyBytes
}

// GetPrimaryKeyBytes returns the number of bytes needed by primaryKey
func (j *JobConfig) GetPrimaryKeyBytes() int {
	return j.primaryKeyBytes
}

// GetColumnDict returns a job's columnDict definition
func (j *JobConfig) GetColumnDict() map[string]int {
	return j.columnDict
}

// PopulateAresTableConfig populates information into jobConfig fields
func (j *JobConfig) PopulateAresTableConfig() error {
	// set primaryKeys and primaryKeyBytes
	j.primaryKeyBytes = 0
	j.primaryKeys = make(map[string]int, len(j.AresTableConfig.Table.PrimaryKeyColumns))
	for _, pk := range j.AresTableConfig.Table.PrimaryKeyColumns {
		j.primaryKeys[j.AresTableConfig.Table.Columns[pk].Name] = pk
		columnType := j.AresTableConfig.Table.Columns[pk].Type
		dataBits := memCom.DataTypeBits(memCom.DataTypeFromString(columnType))
		if dataBits < 8 {
			dataBits = 8
		}
		j.primaryKeyBytes += dataBits / 8
	}

	size := len(j.AresTableConfig.Table.Columns)
	j.destinations = make(map[string]*DestinationConfig, size)
	j.transformations = make(map[string]*TransformationConfig, size)
	j.columnDict = make(map[string]int, size)

	// set destinations and transformations
	for columnID, column := range j.AresTableConfig.Table.Columns {
		if column.Deleted {
			continue
		}
		j.columnDict[column.Name] = columnID

		updateMode := j.getUpdateMode(column.Name)
		j.destinations[column.Name] = &DestinationConfig{
			Table:      j.AresTableConfig.Table.Name,
			Column:     column.Name,
			UpdateMode: updateMode,
		}

		var defaultValue string
		if column.DefaultValue != nil {
			defaultValue = *column.DefaultValue
		}
		j.transformations[column.Name] = &TransformationConfig{
			Type:    column.Type,
			Source:  column.Name,
			Default: defaultValue,
		}
	}
	return nil
}

func (j *JobConfig) getUpdateMode(column string) memCom.ColumnUpdateMode {
	updateMode := memCom.UpdateOverwriteNotNull
	if _, ok := j.primaryKeys[column]; ok {
		updateMode = memCom.UpdateOverwriteNotNull
	} else if modeStr, ok := j.AresTableConfig.UpdateMode[column]; ok {
		updateMode = parseUpdateMode(modeStr)
	}
	return updateMode
}

// AddLocalJobConfig creates a list of jobConfigs from local configuration file
func AddLocalJobConfig(serviceConfig config.ServiceConfig, jobConfigs JobConfigs) error {
	serviceConfig.Logger.Info("Start AddLocalJobConfig")

	// iterate all active jobs configured at local
	for _, jobName := range serviceConfig.ActiveJobs {
		serviceConfig.Logger.Info(fmt.Sprintf("Loading job: %s", jobName))

		// set the job configure by loading its configuration file
		jobConfFile := fmt.Sprintf(
			"%s/%s/jobs/%s-%s.json",
			config.ConfigRootPath, serviceConfig.Environment.RuntimeEnvironment, jobName, serviceConfig.Environment.Zone)
		serviceConfig.Logger.Info(fmt.Sprintf("Loading job config file: %s", jobConfFile))

		jobConf, err := ioutil.ReadFile(jobConfFile)
		if err != nil {
			if os.IsNotExist(err) {
				serviceConfig.Logger.Warn(fmt.Sprintf("No job config file: %s", jobConfFile))
				continue
			}
			serviceConfig.Logger.Error(
				fmt.Sprintf("Failed to read job config file: %s", jobConfFile), zap.Error(err))
			return err
		}

		jobConfig, err := newJobConfig(jobConf)
		if err != nil {
			serviceConfig.Logger.Error("Failed to load job config",
				zap.String("job", jobName),
				zap.Error(err))
			return err
		}
		serviceConfig.Logger.Info(fmt.Sprintf("Loaded job config file: %s", jobConfFile))

		// iterate all active ares cluster to set jobConfig for each of them
		jobAresConfig := make(JobAresConfig)
		for aresCluster := range serviceConfig.ActiveAresClusters {
			aresClusterJobConfig, err := CloneJobConfig(jobConfig, serviceConfig, aresCluster)
			if err != nil {
				serviceConfig.Logger.Error("Failed to copy job config",
					zap.String("job", jobName),
					zap.String("aresCluster", aresCluster),
					zap.Error(err))
				return err
			}
			jobAresConfig[aresCluster] = aresClusterJobConfig
		}
		jobConfigs[jobName] = jobAresConfig
		serviceConfig.Logger.Info(fmt.Sprintf("Loaded job: %s", jobName))
	}
	serviceConfig.Logger.Info("Done AddLocalJobConfig")
	return nil
}

// newJobConfig creates a jobConfig from json
func newJobConfig(value []byte) (*JobConfig, error) {
	var jobConfig JobConfig

	err := json.Unmarshal(value, &jobConfig)
	if err != nil {
		return nil, err
	}

	err = jobConfig.PopulateAresTableConfig()
	return &jobConfig, err
}

// CloneJobConfig deep copy jobConfig
func CloneJobConfig(src *JobConfig, serviceConfig config.ServiceConfig, aresCluster string) (*JobConfig, error) {
	serviceConfig.Logger.Info("Copying job config",
		zap.String("job", src.Name),
		zap.String("aresCluster", aresCluster))

	dst := new(JobConfig)
	if err := deepcopy.Copy(dst, src); err != nil {
		return nil, err
	}

	// copy column defaultValue
	for i, column := range src.AresTableConfig.Table.Columns {
		if column.DefaultValue == nil {
			continue
		}
		defaultValue := *column.DefaultValue
		dst.AresTableConfig.Table.Columns[i].DefaultValue = &defaultValue

	}

	// copy destinations map
	dst.destinations = make(map[string]*DestinationConfig, len(src.destinations))
	for k, v := range src.destinations {
		if v == nil {
			continue
		}
		dst.destinations[k] = new(DestinationConfig)
		if err := deepcopy.Copy(dst.destinations[k], v); err != nil {
			return nil, err
		}
	}

	// copy transformations map
	dst.transformations = make(map[string]*TransformationConfig, len(src.transformations))
	for k, v := range src.transformations {
		if v == nil {
			continue
		}
		dst.transformations[k] = new(TransformationConfig)
		if err := deepcopy.Copy(dst.transformations[k], v); err != nil {
			return nil, err
		}
		dst.transformations[k].Context = make(map[string]string, len(v.Context))
		for ck, ctx := range v.Context {
			dst.transformations[k].Context[ck] = ctx
		}
	}

	// copy primaryKeys map[string]interface{}
	dst.primaryKeys = make(map[string]int, len(src.primaryKeys))
	for k, v := range src.primaryKeys {
		dst.primaryKeys[k] = v
	}

	dst.AresTableConfig.Cluster = aresCluster
	serviceConfig.Logger.Info("Copied job config",
		zap.String("job", src.Name),
		zap.String("aresCluster", aresCluster))
	return dst, nil
}

// parseUpdateMode converts update mode string to memCom.ColumnUpdateMode
func parseUpdateMode(modeStr string) memCom.ColumnUpdateMode {
	switch strings.ToLower(modeStr) {
	case "overwrite_notnull":
		return memCom.UpdateOverwriteNotNull
	case "overwrite_force":
		return memCom.UpdateForceOverwrite
	case "addition":
		return memCom.UpdateWithAddition
	case "min":
		return memCom.UpdateWithMin
	case "max":
		return memCom.UpdateWithMax
	default:
		return memCom.UpdateOverwriteNotNull
	}
}

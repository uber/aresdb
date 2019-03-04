package rules

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/getlantern/deepcopy"
	"github.com/uber/aresdb/client"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
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

// JobConfig contains configuration and information related to the job
type JobConfig struct {
	// Name is job name
	Name string `json:"job" yaml:"job"`
	// Version is jobConfig version
	Version int `json:"version" yaml:"version"`
	// StreamingConfig contains kafka consumer configuration and incoming message schema definition
	StreamingConfig StreamingConfig `json:"streamConfig" yaml:"streamConfig"`
	// AresTableConfig contains destination ares table schema
	AresTableConfig AresTableConfig `json:"aresTableConfig" yaml:"aresTableConfig"`

	destinations    map[string]*DestinationConfig
	transformations map[string]*TransformationConfig
	primaryKeys     map[string]interface{}
}

// AresTableConfig contains ares table schema and cluster informaiton
type AresTableConfig struct {
	// Cluster is ares cluster name
	Cluster string `json:"cluster" yaml:"cluster"`
	// Table is ares table name
	Table metaCom.Table `json:"schema" yaml:"schema"`
	// UpdateMode defines column upsert mode
	UpdateMode map[string]string `json:"updateMode"`
}

// StreamingConfig defines job runtime configuration used by streaming processor
type StreamingConfig struct {
	KafkaBroker           string               `json:"kafkaBroker" yaml:"kafkaBroker"`
	KafkaClusterName      string               `json:"kafkaClusterName" yaml:"kafkaClusterName"`
	KafkaClusterFile      string               `json:"kafkaClusterFile" yaml:"kafkaClusterFile"`
	KafkaVersion          string               `json:"kafkaVersion" yaml:"kafkaVersion"`
	ChannelBufferSize     uint                 `json:"channelBufferSize" yaml:"channelBufferSize" default:"256"`
	MaxPollIntervalMs     int                  `json:"maxPollIntervalMs" yaml:"maxPollIntervalMs" default:"100"`
	SessionTimeoutNs      int                  `json:"sessionTimeoutNs" yaml:"sessionTimeoutNs" default:"6000"`
	Topic                 string               `json:"topic" yaml:"topic"`
	TopicType             string               `json:"topicType" yaml:"topicType"`
	LatestOffset          bool                 `json:"latestOffset" yaml:"latestOffset"`
	ErrorThreshold        int                  `json:"errorThreshold" yaml:"errorThreshold"`
	StatusCheckInterval   int                  `json:"statusCheckInterval" yam:"statusCheckInterval"`
	AutoRecoveryThreshold int                  `json:"autoRecoveryThreshold" yaml:"autoRecoveryThreshold"`
	ProcessorCount        int                  `json:"processorCount" yaml:"processorCount"`
	BatchSize             int                  `json:"batchSize" yaml:"batchSize"`
	MaxBatchDelayMS       int                  `json:"maxBatchDelayMS" yaml:"maxBatchDelayMS"`
	MegaBytePerSec        int                  `json:"megaBytePerSec" yaml:"megaBytePerSec"`
	RestartOnFailure      bool                 `json:"restartOnFailure" yaml:"restartOnFailure"`
	RestartInterval       int                  `json:"restartInterval" yaml:"restartInterval"`
	FailureHandler        FailureHandlerConfig `json:"failureHandler"`
}

// FailureHandlerConfig holds the configurations for FailureHandler
type FailureHandlerConfig struct {
	Type   string                    `json:"type" yaml:"type"`
	Config RetryFailureHandlerConfig `json:"config" yaml:"config"`
}

// RetryFailureHandlerConfig holds the configurations for RetryFailureHandler
type RetryFailureHandlerConfig struct {
	InitRetryIntervalInSeconds int     `json:"initRetryIntervalInSeconds" yaml:"initRetryIntervalInSeconds"`
	Multiplier                 float32 `json:"multiplier" yaml:"multiplier"`
	MaxRetryMinutes            int     `json:"maxRetryMinutes" yaml:"maxRetryMinutes"`
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
	AresClusters map[string]client.ConnectorConfig `json:"instances"`
}

// NewJobConfigs creates JobConfigs object
func NewJobConfigs(params Params) (Result, error) {
	jobConfigs := make(JobConfigs)
	err := AddLocalJobConfig(params.ServiceConfig, jobConfigs)

	return Result{
		JobConfigs: jobConfigs,
	}, err

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
func (j *JobConfig) GetPrimaryKeys() map[string]interface{} {
	return j.primaryKeys
}

// PopulateAresTableConfig populates information into jobConfig fields
func (j *JobConfig) PopulateAresTableConfig() error {
	// set primaryKeys
	j.primaryKeys = make(map[string]interface{}, len(j.AresTableConfig.Table.PrimaryKeyColumns))
	for _, pk := range j.AresTableConfig.Table.PrimaryKeyColumns {
		j.primaryKeys[j.AresTableConfig.Table.Columns[pk].Name] = pk
	}

	size := len(j.AresTableConfig.Table.Columns)
	j.destinations = make(map[string]*DestinationConfig, size)
	j.transformations = make(map[string]*TransformationConfig, size)

	// set destinations and transformations
	for _, column := range j.AresTableConfig.Table.Columns {
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
			"config/%s/jobs/%s-%s.json",
			serviceConfig.Environment.RuntimeEnvironment, jobName, serviceConfig.Environment.Zone)
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
	dst.primaryKeys = make(map[string]interface{}, len(src.primaryKeys))
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

package message

import (
	"fmt"
	"runtime"
	"sort"

	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	aresMemCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/subscriber/common/database"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"go.uber.org/zap"
)

// Parser holds all resources needed to parse one message
// into one or multiple row objects with respect to different destinations
type Parser struct {
	// ServiceConfig is ares-subscriber configure
	ServiceConfig config.ServiceConfig
	// JobName is job name
	JobName string
	// Cluster is ares cluster name
	Cluster string
	// destinations each message will be parsed and written into
	Destination database.Destination
	// Transformations are keyed on the output column name
	Transformations map[string]*rules.TransformationConfig
	scope           tally.Scope
}

// NewParser will create a Parser for given JobConfig
func NewParser(jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig) *Parser {
	mp := &Parser{
		ServiceConfig:   serviceConfig,
		JobName:         jobConfig.Name,
		Cluster:         jobConfig.AresTableConfig.Cluster,
		Transformations: jobConfig.GetTranformations(),
		scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": jobConfig.AresTableConfig.Cluster,
		}),
	}
	mp.populateDestination(jobConfig)
	return mp
}

func (mp *Parser) populateDestination(jobConfig *rules.JobConfig) {
	columnNames := []string{}
	updateModes := []aresMemCom.ColumnUpdateMode{}
	destinations := jobConfig.GetDestinations()

	for _, dstConfig := range destinations {
		columnNames = append(columnNames, dstConfig.Column)
	}

	// sort column names in destination for consistent query order
	sort.Strings(columnNames)
	for _, column := range columnNames {
		updateModes = append(updateModes, destinations[column].UpdateMode)
	}

	mp.Destination = database.Destination{
		Table:           jobConfig.AresTableConfig.Table.Name,
		ColumnNames:     columnNames,
		PrimaryKeys:     jobConfig.GetPrimaryKeys(),
		AresUpdateModes: updateModes,
	}
}

// ParseMessage will parse given message to fit the destination
func (mp *Parser) ParseMessage(msg map[string]interface{}, destination database.Destination) (client.Row, error) {
	mp.ServiceConfig.Logger.Debug("Parsing", zap.Any("msg", msg))
	var row client.Row
	for _, col := range destination.ColumnNames {
		transformation := mp.Transformations[col]
		fromValue := mp.extractSourceFieldValue(msg, col)
		toValue, err := transformation.Transform(fromValue)
		if err != nil {
			mp.ServiceConfig.Logger.Error("Tranformation error",
				zap.String("job", mp.JobName),
				zap.String("cluster", mp.Cluster),
				zap.String("field", col),
				zap.String("name", GetFuncName()),
				zap.Error(err))
		}
		row = append(row, toValue)
	}
	return row, nil
}

func (mp *Parser) extractSourceFieldValue(msg map[string]interface{}, fieldName string) interface{} {
	value, err := mp.getValue(msg, fieldName)
	if err != nil {
		mp.ServiceConfig.Logger.Debug("Failed to get value for",
			zap.String("job", mp.JobName),
			zap.String("cluster", mp.Cluster),
			zap.String("field", fieldName),
			zap.String("name", GetFuncName()),
			zap.Error(err))
	}
	return value
}

func (mp *Parser) getValue(msg map[string]interface{}, fieldName string) (interface{}, error) {
	if value, found := msg[fieldName]; found {
		return value, nil
	}
	return nil,
		fmt.Errorf("Message does not contain key: %s, job: %s, cluster: %s", fieldName, mp.JobName, mp.Cluster)
}

//GetFuncName get the function name of the calling function
func GetFuncName() string {
	p, _, _, _ := runtime.Caller(1)
	fn := runtime.FuncForPC(p).Name()
	return fn
}

package metastore

import (
	controllerCom "github.com/uber/aresdb/controller/common"
	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"reflect"
	"time"
)

// SchemaFetchJob is a job that periodically pings ares-controller and updates table schemas if applicable
type SchemaFetchJob struct {
	clusterName       string
	hash              string
	intervalInSeconds int
	schemaMutator     TableSchemaMutator
	schemaValidator   TableSchemaValidator
	controllerClient  controllerCom.ControllerClient
	stopChan          chan struct{}
}

// NewSchemaFetchJob creates a new SchemaFetchJob
func NewSchemaFetchJob(intervalInSeconds int, schemaMutator TableSchemaMutator, schemaValidator TableSchemaValidator, controllerClient controllerCom.ControllerClient, clusterName, initialHash string) *SchemaFetchJob {
	return &SchemaFetchJob{
		clusterName:       clusterName,
		hash:              initialHash,
		intervalInSeconds: intervalInSeconds,
		schemaMutator:     schemaMutator,
		schemaValidator:   schemaValidator,
		stopChan:          make(chan struct{}),
		controllerClient:  controllerClient,
	}
}

// Run starts the scheduling
func (j *SchemaFetchJob) Run() {
	tickChan := time.NewTicker(time.Second * time.Duration(j.intervalInSeconds)).C

	for {
		select {
		case <-tickChan:
			j.FetchSchema()
		case <-j.stopChan:
			return
		}
	}
}

// Stop stops the scheduling
func (j *SchemaFetchJob) Stop() {
	close(j.stopChan)
}

func (j *SchemaFetchJob) FetchSchema() {
	newHash, err := j.controllerClient.GetSchemaHash(j.clusterName)
	if err != nil {
		reportError(err)
		return
	}
	if newHash != j.hash {
		newSchemas, err := j.controllerClient.GetAllSchema(j.clusterName)
		if err != nil {
			reportError(err)
			return
		}
		err = j.applySchemaChange(newSchemas)
		if err != nil {
			reportError(err)
			return
		}
		j.hash = newHash
	}
	utils.GetLogger().Info("Succeeded to run schema fetch job")
	utils.GetRootReporter().GetCounter(utils.SchemaFetchSuccess).Inc(1)
}

func (j *SchemaFetchJob) applySchemaChange(tables []common.Table) (err error) {
	oldTables, err := j.schemaMutator.ListTables()
	if err != nil {
		return
	}

	oldTablesMap := make(map[string]bool)
	for _, oldTableName := range oldTables {
		oldTablesMap[oldTableName] = true
	}

	for _, table := range tables {
		if _, exist := oldTablesMap[table.Name]; !exist {
			// found new table
			err = j.schemaMutator.CreateTable(&table)
			if err != nil {
				return
			}
			utils.GetRootReporter().GetCounter(utils.SchemaCreationCount).Inc(1)
		} else {
			var oldTable *common.Table
			oldTable, err = j.schemaMutator.GetTable(table.Name)
			if err != nil {
				return
			}
			if oldTable.Incarnation < table.Incarnation {
				// found new table incarnation, delete previous table and data
				// then create new table
				err := j.schemaMutator.DeleteTable(table.Name)
				if err != nil {
					return err
				}
				utils.GetRootReporter().GetCounter(utils.SchemaDeletionCount).Inc(1)
				err = j.schemaMutator.CreateTable(&table)
				if err != nil {
					return err
				}
				utils.GetRootReporter().GetCounter(utils.SchemaCreationCount).Inc(1)
			} else if oldTable.Incarnation == table.Incarnation && !reflect.DeepEqual(&table, oldTable) {
				// found table update
				j.schemaValidator.SetNewTable(table)
				j.schemaValidator.SetOldTable(*oldTable)
				err = j.schemaValidator.Validate()
				if err != nil {
					return
				}
				err = j.schemaMutator.UpdateTable(table)
				if err != nil {
					return
				}
				utils.GetRootReporter().GetCounter(utils.SchemaUpdateCount).Inc(1)
			}
			oldTablesMap[table.Name] = false
		}
	}

	for oldTableName, notAddressed := range oldTablesMap {
		if notAddressed {
			// found table deletion
			err = j.schemaMutator.DeleteTable(oldTableName)
			if err != nil {
				return
			}
			utils.GetRootReporter().GetCounter(utils.SchemaDeletionCount).Inc(1)
		}
	}

	return
}

func reportError(err error) {
	utils.GetRootReporter().GetCounter(utils.SchemaFetchFailure).Inc(1)
	utils.GetLogger().Error(utils.StackError(err, "err running schema fetch job"))
}

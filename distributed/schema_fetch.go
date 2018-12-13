package distributed

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
)

// SchemaFetchJob watches schema changes in zk and apply changes locally
type SchemaFetchJob struct {
	clusterName     string
	schemaRootZnode string
	schemaMutator   metastore.TableSchemaMutator
	schemaValidator metastore.TableSchemaValidator
	zkc             *zk.Conn
	// node update event and schema root children change events
	eventChan chan zk.Event
	stopChan chan struct{}
}

// NewSchemaFetchJob creates a new SchemaFetchJob
func NewSchemaFetchJob(schemaMutator metastore.TableSchemaMutator, schemaValidator metastore.TableSchemaValidator, clusterName string, zkc *zk.Conn) *SchemaFetchJob {
	return &SchemaFetchJob{
		clusterName:     clusterName,
		schemaRootZnode: fmt.Sprintf("/ares_controller/%s/schema", clusterName),
		schemaMutator:   schemaMutator,
		schemaValidator: schemaValidator,
		zkc:             zkc,
		eventChan:       make(chan zk.Event, 1),
		stopChan: make(chan struct{}),
	}
}

// Run starts the job
func (j *SchemaFetchJob) Run() {
	go watchChildren(j.schemaRootZnode, j.zkc, j.eventChan, j.stopChan)

	for {
		select {
		case event := <-j.eventChan:
			switch event.Type {
			case zk.EventNodeChildrenChanged:
				err := j.FetchApplySchema(false)
				if err != nil {
					reportError(err)
					return
				}
			case zk.EventNodeDataChanged:
				table, err := j.getTableFromPath(event.Path)
				if err != nil {
					reportError(err)
				}
				err = j.updateTable(table)
				if err != nil {
					reportError(err)
				}
			}
		case <-j.stopChan:
			utils.GetLogger().Info("stopping schema fetch event loop")
			return
		}
	}
}

// Stop the job
func (j *SchemaFetchJob) Stop() {
	j.stopChan <- struct{}{}
}


func (j *SchemaFetchJob) FetchApplySchema(bootstrap bool) (err error) {
	remoteTables, err := j.getAllSchema()
	if err != nil {
		return
	}

	err = j.applySchemaChange(remoteTables, bootstrap)
	reportSuccess()
	return
}

func (j *SchemaFetchJob) getAllSchema() (tables []common.Table, err error) {
	tableNames, _, err := j.zkc.Children(j.schemaRootZnode)
	if err != nil {
		return
	}

	tables = make([]common.Table, len(tableNames))

	for i, tableName := range tableNames {
		tablePath := fmt.Sprintf("%s/%s", j.schemaRootZnode, tableName)
		var table common.Table
		table, err = j.getTableFromPath(tablePath)
		if err != nil {
			return
		}
		tables[i] = table
	}
	return
}

func (j *SchemaFetchJob) getTableFromPath(path string) (table common.Table, err error) {
	tableBytes, _, err := j.zkc.Get(path)
	if err != nil {
		return
	}
	err = json.Unmarshal(tableBytes, &table)
	return
}

// applySchemaChange applies a snapshot of all tables to local schema store
func (j *SchemaFetchJob) applySchemaChange(tables []common.Table, bootstrap bool) (err error) {
	oldTables, err := j.schemaMutator.ListTables()
	if err != nil {
		return
	}

	oldTablesMap := make(map[string]bool)
	for _, oldTableName := range oldTables {
		oldTablesMap[oldTableName] = true
	}

	for _, table := range tables {
		if bootstrap {
			go watchPath(fmt.Sprintf("%s/%s", j.schemaRootZnode, table.Name), j.zkc, j.eventChan, j.stopChan)
		}
		if !oldTablesMap[table.Name] {
			// found new table
			err = j.schemaMutator.CreateTable(&table)
			if err != nil {
				return
			}
			if !bootstrap {
				go watchPath(fmt.Sprintf("%s/%s", j.schemaRootZnode, table.Name), j.zkc, j.eventChan, j.stopChan)
			}
			utils.GetRootReporter().GetCounter(utils.SchemaCreationCount).Inc(1)
		} else {
			// existing table, potentially update. mark non delete.
			oldTablesMap[table.Name] = false
			err = j.updateTable(table)
			if err != nil {
				reportError(err)
				err = nil
				continue
			}
		}
	}

	for oldTableName, notAddressed := range oldTablesMap {
		if notAddressed {
			// found table deletion
			err = j.schemaMutator.DeleteTable(oldTableName)
			if err != nil {
				reportError(err)
				err = nil
				continue
			}
			utils.GetRootReporter().GetCounter(utils.SchemaDeletionCount).Inc(1)
		}
	}

	return
}

func (j *SchemaFetchJob) updateTable(newTable common.Table) (err error) {
	var oldTable *common.Table
	oldTable, err = j.schemaMutator.GetTable(newTable.Name)
	if err != nil {
		err = utils.StackError(err, "failed to get existing table")
		return
	}
	j.schemaValidator.SetNewTable(newTable)
	j.schemaValidator.SetOldTable(*oldTable)
	err = j.schemaValidator.Validate()
	if err != nil {
		return
	}
	err = j.schemaMutator.UpdateTable(newTable)
	if err != nil {
		return
	}
	utils.GetRootReporter().GetCounter(utils.SchemaUpdateCount).Inc(1)
	return
}

// watchPath keeps watching a znode, forwarding data change events to outChan until:
// 1. znode was deleted, in which case it will stop watching and exit silently
// 2. other error happened, in which case it will report error and exit
func watchPath(path string, zkc *zk.Conn, outChan chan zk.Event, stopChan chan struct{}) {
	utils.GetLogger().WithField("path", path).Info("Watching zk data")

	var err error
	var watchChan <-chan zk.Event
	// a new chan has to be created because zk watch can only trigger once
	for {
		_, _, watchChan, err = zkc.GetW(path)
		if err != nil {
			if err != zk.ErrNoNode {
				reportError(err)
			}
			return
		}

		select {
		case event := <-watchChan:
			if event.Type == zk.EventNodeDataChanged {
				outChan <- event
			}
		case <-stopChan:
			utils.GetLogger().WithField("path", path).Info("stop watching")
			return
		}
	}
}

// watchChildren keeps watching a znode's children, forwarding children change events to outChan
func watchChildren(path string, zkc *zk.Conn, outChan chan zk.Event, stopChan chan struct{}) {
	utils.GetLogger().WithField("path", path).Info("Watching zk children")

	var err error
	var watchChan <-chan zk.Event
	for {
		_, _, watchChan, err = zkc.ChildrenW(path)
		if err != nil {
			reportError(err)
			return
		}
		select {
		case event := <-watchChan:
			outChan <- event
		case <-stopChan:
			utils.GetLogger().WithField("path", path).Info("stop watching")
			return
		}
	}
}

func reportError(err error) {
	utils.GetRootReporter().GetCounter(utils.SchemaFetchFailure).Inc(1)
	utils.GetLogger().Error(utils.StackError(err, "err running schema fetch job"))
}

func reportSuccess() {
	utils.GetLogger().Info("Succeeded to fetch and apply schema")
	utils.GetRootReporter().GetCounter(utils.SchemaFetchSuccess).Inc(1)
}

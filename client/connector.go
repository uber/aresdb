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

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/uber-go/tally"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"unsafe"
)

const (
	// default request time out in seconds
	defaultRequestTimeout = 5
	// default schema refresh interval in seconds
	defaultSchemaRefreshInterval = 600
	dataIngestionHeader          = "application/upsert-data"
	applicationJSONHeader        = "application/json"
)

// Row represents a row of insert data.
type Row []interface{}

// Connector is the connector interface for ares.
type Connector interface {
	// Insert inserts rows to ares
	// returns number of rows inserted and error.
	// updateModes are optional, if ignored for all columns, no need to set
	// if set, then all columns needs to be set
	Insert(tableName string, columnNames []string, rows []Row, updateModes ...memCom.ColumnUpdateMode) (int, error)
}

// UpsertBatchBuilder is an interface of upsertBatch on client side
type UpsertBatchBuilder interface {
	// SetTableSchema sets table schema.
	SetTableSchema(table metaCom.Table, enumMappings map[int]enumDict, enumDefaultValueMappings map[int]int) *TableSchema

	// PrepareUpsertBatch prepares the upsert batch for upsert, returns upsertBatch byte array, number of rows in upsert batch and error.
	PrepareUpsertBatch(tableName string, columnNames []string, updateModes []memCom.ColumnUpdateMode, rows []Row) ([]byte, int, error)
}

// enumCasesWrapper is a response/request body which wraps enum cases
type enumCasesWrapper struct {
	EnumCases []string
}

type TableSchema struct {
	Table *metaCom.Table
	// maps from column name to columnID for convenience
	ColumnDict map[string]int
}

// enumDict maps from enum value to enumID
type enumDict map[string]int

// UpsertBatchBuilderImpl implements interface UpsertBatchBuilder
type UpsertBatchBuilderImpl struct {
	sync.RWMutex

	cfg         ConnectorConfig
	logger      *zap.SugaredLogger
	metricScope tally.Scope

	// mapping from table name to table schema
	schemas map[string]*TableSchema
	// map from table to columnID to enum dictionary
	// use columnID instead of name since column name can be reused
	// table names can be reused as well, deleting and adding a new table
	// will anyway requires job restart
	enumMappings map[string]map[int]enumDict

	// map from table to columnID to default enum id. Initialized during bootstrap
	// and will be set only if default value is non nil.
	enumDefaultValueMappings map[string]map[int]int
}

// connector is the ares connector implementation
type connector struct {
	*UpsertBatchBuilderImpl

	httpClient http.Client
}

// ConnectorConfig holds the configurations for ares Connector.
type ConnectorConfig struct {
	// Address is in the format of host:port
	Address string `yaml:"address"`
	// DeviceChoosingTimeout value is the request timeout in seconds for http calls
	// if <= 0, will use default
	Timeout int `yaml:"timeout"`
	// SchemaRefreshInterval is the interval in seconds for the connector to
	// fetch and refresh schema from ares
	// if <= 0, will use default
	SchemaRefreshInterval int `yaml:"schemaRefreshInterval"`
}

// NewConnector returns a new ares Connector
func (cfg ConnectorConfig) NewConnector(logger *zap.SugaredLogger, metricScope tally.Scope) (Connector, error) {
	if cfg.SchemaRefreshInterval <= 0 {
		cfg.SchemaRefreshInterval = defaultSchemaRefreshInterval
	}

	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultRequestTimeout
	}

	connectorCommon := UpsertBatchBuilderImpl{
		cfg:                      cfg,
		logger:                   logger,
		metricScope:              metricScope,
		schemas:                  make(map[string]*TableSchema),
		enumMappings:             make(map[string]map[int]enumDict),
		enumDefaultValueMappings: make(map[string]map[int]int),
	}
	connector := &connector{
		UpsertBatchBuilderImpl: &connectorCommon,
	}

	connector.initHTTPClient()

	err := connector.fetchAllTables()
	if err != nil {
		return nil, err
	}

	go func(refreshInterval int) {
		ticks := time.Tick(time.Duration(refreshInterval) * time.Second)
		for range ticks {
			err = connector.fetchAllTables()
			if err != nil {
				logger.With(
					"error", err.Error()).Errorf("Failed to fetch table schema")
			}
		}
	}(cfg.SchemaRefreshInterval)

	return connector, nil
}

func (c *connector) initHTTPClient() {
	c.httpClient = http.Client{
		Timeout: time.Duration(c.cfg.Timeout) * time.Second,
	}
}

// Insert inserts a batch of rows into ares
func (c *connector) Insert(tableName string, columnNames []string, rows []Row, updateModes ...memCom.ColumnUpdateMode) (int, error) {
	if len(columnNames) == 0 {
		return 0, utils.StackError(nil, "No column names specified")
	}

	// if no update modes at all, use default
	if len(updateModes) == 0 {
		updateModes = make([]memCom.ColumnUpdateMode, len(columnNames))
	}

	if len(updateModes) != len(columnNames) {
		return 0, utils.StackError(nil, "length of column update modes %d does not equal to number of columns %d", len(updateModes), len(columnNames))
	}

	if len(rows) == 0 {
		// Do nothing when there is no row to insert
		return 0, nil
	}

	for _, row := range rows {
		if len(row) != len(columnNames) {
			return 0, utils.StackError(nil,
				"Length of column names should match length of a single row, length of column names :%d, length of row: %d",
				len(columnNames),
				len(row),
			)
		}
	}

	upsertBatchBytes, numRows, err := c.prepareUpsertBatch(tableName, columnNames, updateModes, rows)
	if err != nil {
		return numRows, err
	}

	//TODO: currently always use shard zero for single instance version
	resp, err := c.httpClient.Post(c.dataPath(tableName, 0), dataIngestionHeader, bytes.NewReader(upsertBatchBytes))
	if err != nil || resp.StatusCode != http.StatusOK {
		//TODO: break status code check and error check into two parts for more specific handling like retrying on 5xx
		return 0, utils.StackError(err, "Failed to post upsert batch, table: %s, shard: %d", tableName, 0)
	}

	return numRows, nil
}

// computeHLLValue populate hyperloglog value
func computeHLLValue(dataType memCom.DataType, value interface{}) (uint32, error) {
	var ok bool
	var hashed uint64
	switch dataType {
	case memCom.UUID:
		var v [2]uint64
		v, ok = memCom.ConvertToUUID(value)
		hashed = v[0] ^ v[1]
	case memCom.Uint32:
		var v uint32
		v, ok = memCom.ConvertToUint32(value)
		hashed = utils.Murmur3Sum64(unsafe.Pointer(&v), memCom.DataTypeBytes(dataType), 0)
	case memCom.Int32:
		var v int32
		v, ok = memCom.ConvertToInt32(value)
		hashed = utils.Murmur3Sum64(unsafe.Pointer(&v), memCom.DataTypeBytes(dataType), 0)
	case memCom.Int64:
		var v int64
		v, ok = memCom.ConvertToInt64(value)
		hashed = utils.Murmur3Sum64(unsafe.Pointer(&v), memCom.DataTypeBytes(dataType), 0)
	default:
		return 0, utils.StackError(nil, "invalid type %s for fast hll value", memCom.DataTypeName[dataType])
	}
	if !ok {
		return 0, utils.StackError(nil, "invalid data value %v for data type %s", value, memCom.DataTypeName[dataType])
	}
	return utils.ComputeHLLValue(hashed), nil
}

// prepareUpsertBatch prepares the upsert batch for upsert,
// returns upsertBatch byte array, number of rows in upsert batch and error.
func (c *connector) prepareUpsertBatch(tableName string, columnNames []string, updateModes []memCom.ColumnUpdateMode, rows []Row) ([]byte, int, error) {
	schema, err := c.getTableSchema(tableName)
	if err != nil {
		return nil, 0, err
	}

	// return error if primary key is missing
	if err = c.checkPrimaryKeys(schema, columnNames); err != nil {
		return nil, 0, err
	}

	// return error if time column is missing
	if err = c.checkTimeColumnExistence(schema, columnNames); err != nil {
		return nil, 0, err
	}

	return c.PrepareUpsertBatch(tableName, columnNames, updateModes, rows)
}

// checkPrimaryKeys checks whether primary key is missing
func (u *UpsertBatchBuilderImpl) checkPrimaryKeys(schema *TableSchema, columnNames []string) error {
	for _, columnID := range schema.Table.PrimaryKeyColumns {
		pkColumn := schema.Table.Columns[columnID]
		index := utils.IndexOfStr(columnNames, pkColumn.Name)
		if index < 0 {
			return utils.StackError(nil, "Missing primary key column")
		}
	}
	return nil
}

// checkTimeColumnExistence checks if time column is missing for fact table
func (u *UpsertBatchBuilderImpl) checkTimeColumnExistence(schema *TableSchema, columnNames []string) error {
	if !schema.Table.IsFactTable || schema.Table.Config.AllowMissingEventTime {
		return nil
	}

	for _, columnName := range columnNames {
		columnID, exist := schema.ColumnDict[columnName]
		if !exist {
			continue
		}

		if columnID == 0 {
			return nil
		}
	}
	return utils.StackError(nil, "Missing time column")
}

func (u *UpsertBatchBuilderImpl) translateEnum(tableName string, columnID int, enumCase string, caseInsensitive bool) (enumID int, err error) {
	u.RLock()
	// here it already make sure the enum dictionary exists in cache
	enumID, ok := u.enumMappings[tableName][columnID][enumCase]
	u.RUnlock()
	if !ok {
		u.metricScope.Tagged(
			map[string]string{
				"TableName": tableName,
				"ColumnID":  strconv.Itoa(columnID),
			},
		).Counter("new_enum_case_rows_ignored").Inc(int64(1))
		if defaultValue, ok := u.enumDefaultValueMappings[tableName][columnID]; ok {
			return defaultValue, nil
		}
		return -1, nil
	}
	return enumID, nil
}

func (c *connector) getTableSchema(tableName string) (*TableSchema, error) {
	c.RLock()
	schema, exist := c.schemas[tableName]
	c.RUnlock()
	if exist {
		return schema, nil
	}

	return c.fetchTableSchema(tableName)
}

// fetchAllTables fetches all table schemas from ares,
// this is called during connector initialization.
func (c *connector) fetchAllTables() error {
	var tables []string
	resp, err := c.httpClient.Get(c.listTablesPath())
	err = c.readJSONResponse(resp, err, &tables)
	if err != nil {
		return utils.StackError(err, "Failed to fetch table list")
	}

	for _, tableName := range tables {
		_, err := c.fetchTableSchema(tableName)
		if err != nil {
			c.metricScope.Tagged(map[string]string{
				"table": tableName,
			}).Counter("err_fetch_table").Inc(1)
			return utils.StackError(err, "Failed to fetch schema error")
		}
	}
	return nil
}

// fetchEnumDicts fetches all enum dictionaries for enum columns.
func (c *connector) fetchEnumDicts(table metaCom.Table) (map[int]enumDict, map[int]int, error) {
	enumMappings := make(map[int]enumDict)
	enumDefaultValueMappings := make(map[int]int)
	for columnID, column := range table.Columns {
		if column.Deleted {
			continue
		}
		enumMappings[columnID] = make(enumDict)
		caseInsensitive := column.CaseInsensitive
		var defValuePtr *string

		if column.DefaultValue != nil {
			var defValue = *column.DefaultValue
			if caseInsensitive {
				defValue = strings.ToLower(defValue)
			}
			defValuePtr = &defValue
		}

		if column.IsEnumColumn() && column.DisableAutoExpand {
			enumCases, err := c.fetchEnumDict(table.Name, column.Name)
			if err == nil {
				for enumID, enumCase := range enumCases {
					// Convert to lower case for comparison during ingestion.
					if caseInsensitive {
						enumCase = strings.ToLower(enumCase)
					}
					// all mapping should be pre created
					enumMappings[columnID][enumCase] = enumID

					if defValuePtr != nil {
						if *defValuePtr == enumCase {
							enumDefaultValueMappings[columnID] = enumID
						}
					}
				}
			} else {
				c.metricScope.Tagged(map[string]string{
					"table":    table.Name,
					"columnID": strconv.Itoa(columnID),
				}).Counter("err_fetch_enum_dict").Inc(1)
				return nil, nil, utils.StackError(err, "Failed to fetch enum cases for table: %s, column: %d", table.Name, columnID)
			}
		}
	}
	return enumMappings, enumDefaultValueMappings, nil
}

func (c *connector) fetchEnumDict(tableName, columnName string) ([]string, error) {
	var enumDictReponse []string

	resp, err := c.httpClient.Get(c.enumDictPath(tableName, columnName))

	err = c.readJSONResponse(resp, err, &enumDictReponse)

	return enumDictReponse, err
}

func (c *connector) fetchTableSchema(tableName string) (*TableSchema, error) {
	var table metaCom.Table

	resp, err := c.httpClient.Get(c.tablePath(tableName))
	err = c.readJSONResponse(resp, err, &table)
	if err != nil {
		return nil, err
	}
	enumMappings, enumDefaultValueMappings, err := c.fetchEnumDicts(table)
	if err != nil {
		return nil, err
	}
	return c.SetTableSchema(table, enumMappings, enumDefaultValueMappings), nil
}

func (c *connector) fetchEnumCases(tableName, columnName string) ([]string, error) {
	var enumDictReponse []string

	resp, err := c.httpClient.Get(c.enumDictPath(tableName, columnName))

	err = c.readJSONResponse(resp, err, &enumDictReponse)

	return enumDictReponse, err
}

func (c *connector) readJSONResponse(response *http.Response, err error, data interface{}) error {
	if err != nil {
		return utils.StackError(err, "Failed call remote endpoint")
	}

	respBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return utils.StackError(err, "Failed to read response body")
	}

	if response.StatusCode != http.StatusOK {
		return utils.StackError(nil, "Received error response %d:%s from remote endpoint", response.StatusCode, respBytes)
	}

	err = json.Unmarshal(respBytes, data)
	if err != nil {
		return utils.StackError(err, "Failed to unmarshal json")
	}
	return nil
}

func (c *connector) listTablesPath() string {
	return fmt.Sprintf("http://%s/schema/tables", c.cfg.Address)
}

func (c *connector) tablePath(tableName string) string {
	return fmt.Sprintf("%s/%s", c.listTablesPath(), tableName)
}

func (c *connector) dataPath(tableName string, shard int) string {
	return fmt.Sprintf("http://%s/data/%s/%d", c.cfg.Address, tableName, shard)
}

func (c *connector) enumDictPath(tableName, columnName string) string {
	return fmt.Sprintf("%s/%s/columns/%s/enum-cases", c.listTablesPath(), tableName, columnName)
}

// FetchTableSchema fetches table schemas from ares.
func (u *UpsertBatchBuilderImpl) SetTableSchema(table metaCom.Table, enumMappings map[int]enumDict, enumDefaultValueMappings map[int]int) *TableSchema {
	tableName := table.Name
	columnDict := make(map[string]int)
	for columnID, column := range table.Columns {
		if !column.Deleted {
			columnDict[column.Name] = columnID
		}
	}

	schema := &TableSchema{
		Table:      &table,
		ColumnDict: columnDict,
	}

	u.Lock()
	defer u.Unlock()
	u.schemas[tableName] = schema
	u.enumMappings[tableName] = enumMappings
	u.enumDefaultValueMappings[tableName] = enumDefaultValueMappings

	return schema
}

// PrepareUpsertBatch prepares the upsert batch for upsert,
// returns upsertBatch byte array, number of rows in upsert batch and error.
func (u *UpsertBatchBuilderImpl) PrepareUpsertBatch(tableName string, columnNames []string,
	updateModes []memCom.ColumnUpdateMode, rows []Row) ([]byte, int, error) {
	var err error
	upsertBatchBuilder := memCom.NewUpsertBatchBuilder()

	// use abandonRows to record abandoned row index due to invalid data
	abandonRows := map[int]interface{}{}

	for colIndex, columnName := range columnNames {
		columnID, exist := u.schemas[tableName].ColumnDict[columnName]
		if !exist {
			continue
		}
		column := u.schemas[tableName].Table.Columns[columnID]

		// following conditions only overwrite is supported:
		// 1. dimension table (TODO: might support min/max in the future if needed)
		// 2. primary key column
		// 3. archiving sort column
		// 4. data type not in uint8, int8, uint16, int16, uint32, int32, float32
		if (!u.schemas[tableName].Table.IsFactTable ||
			utils.IndexOfInt(u.schemas[tableName].Table.PrimaryKeyColumns, columnID) >= 0 ||
			utils.IndexOfInt(u.schemas[tableName].Table.ArchivingSortColumns, columnID) >= 0 ||
			u.schemas[tableName].Table.Columns[columnID].IsOverwriteOnlyDataType()) &&
			updateModes[colIndex] > memCom.UpdateForceOverwrite {
			return nil, 0, utils.StackError(nil, "column %s only supports overwrite", columnName)
		}

		dataType := memCom.DataTypeForColumn(column)
		if err = upsertBatchBuilder.AddColumnWithUpdateMode(columnID, dataType, updateModes[colIndex]); err != nil {
			return nil, 0, err
		}
	}

	for rowIndex, row := range rows {
		if _, exist := abandonRows[rowIndex]; exist {
			continue
		}
		upsertBatchBuilder.AddRow()

		upsertBatchColumnIndex := 0
		for inputColIndex, columnName := range columnNames {
			columnID, exist := u.schemas[tableName].ColumnDict[columnName]
			if !exist {
				continue
			}
			column := u.schemas[tableName].Table.Columns[columnID]

			value := row[inputColIndex]

			// prevent primary key being nil
			if value == nil && utils.IndexOfInt(u.schemas[tableName].Table.PrimaryKeyColumns, columnID) >= 0 {
				upsertBatchBuilder.RemoveRow()
				u.logger.With(
					"name", "PrepareUpsertBatch",
					"table", tableName,
					"columnID", columnID,
					"value", value).Error("PrimaryKey column is nil")
				break
			}

			// skip rows if time column is nil for fact table
			if value == nil && u.schemas[tableName].Table.IsFactTable && !u.schemas[tableName].Table.Config.AllowMissingEventTime && columnID == 0 {
				upsertBatchBuilder.RemoveRow()
				u.logger.With(
					"name", "PrepareUpsertBatch",
					"table", tableName,
					"columnID", columnID,
					"value", value).Error("Time column is nil")
				break
			}

			if column.IsEnumColumn() && value != nil {
				enumCase, ok := value.(string)
				if !ok {
					upsertBatchBuilder.RemoveRow()
					u.logger.With(
						"name", "PrepareUpsertBatch",
						"table", tableName,
						"columnID", columnID,
						"value", value).Error("Enum value should be string")
					break
				}
				if column.CaseInsensitive {
					enumCase = strings.ToLower(enumCase)
				}
				value = enumCase
				if column.DisableAutoExpand {
					value, err = u.translateEnum(tableName, columnID, enumCase, column.CaseInsensitive)
					if err != nil {
						upsertBatchBuilder.RemoveRow()
						u.logger.With(
							"name", "PrepareUpsertBatch",
							"error", err.Error(),
							"table", tableName,
							"columnID", columnID,
							"value", value).Error("Failed to translate enum")
						break
					}

					// If enum value is not found from predefined enum cases and default value is not set, we set it to nil.
					if value == -1 {
						value = nil
					}
				}
			}

			// Set value to the last row.
			// compute hll value to insert
			if column.HLLConfig.IsHLLColumn {
				// here use original column data type to compute hll value
				value, err = computeHLLValue(memCom.DataTypeFromString(column.Type), value)
				if err != nil {
					upsertBatchBuilder.RemoveRow()
					u.logger.With("name", "PrepareUpsertBatch", "error", err.Error(), "table", tableName, "columnID", columnID, "value", value).Error("Failed to set value")
					break
				}
				if err = upsertBatchBuilder.SetValue(upsertBatchBuilder.NumRows-1, upsertBatchColumnIndex, value); err != nil {
					upsertBatchBuilder.RemoveRow()
					u.logger.With("name", "PrepareUpsertBatch", "error", err.Error(), "table", tableName, "columnID", columnID, "value", value).Error("Failed to set value")
					break
				}
			} else {
				// directly insert value
				if err = upsertBatchBuilder.SetValue(upsertBatchBuilder.NumRows-1, upsertBatchColumnIndex, value); err != nil {
					upsertBatchBuilder.RemoveRow()
					u.logger.With("name", "PrepareUpsertBatch", "error", err.Error(), "table", tableName, "columnID", columnID, "value", value).Error("Failed to set value")
					break
				}
			}
			upsertBatchColumnIndex++
		}
	}

	batchBytes, err := upsertBatchBuilder.ToByteArray()
	return batchBytes, upsertBatchBuilder.NumRows, err
}

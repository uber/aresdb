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

type tableSchema struct {
	Table *metaCom.Table
	// maps from column name to columnID for convenience
	ColumnDict map[string]int
}

// enumDict maps from enum value to enumID
type enumDict map[string]int

// connector is the ares connector implementation
type connector struct {
	sync.RWMutex

	cfg         ConnectorConfig
	httpClient  http.Client
	logger      *zap.SugaredLogger
	metricScope tally.Scope

	// mapping from table name to table schema
	schemas map[string]*tableSchema
	// map from table to columnID to enum dictionary
	// use columnID instead of name since column name can be reused
	// table names can be reused as well, deleting and adding a new table
	// will anyway requires job restart
	// enum mapping is only needed for enum columns with auto expand disabled for enum translation
	enumMappings map[string]map[int]enumDict

	// map from table to columnID to default enum id. Initialized during bootstrap
	// and will be set only if default value is non nil.
	// enumDefaultValueMappings is only needed for enum columns with auto expand disabled
	enumDefaultValueMappings map[string]map[int]int
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

	connector := &connector{
		cfg:                      cfg,
		logger:                   logger,
		metricScope:              metricScope,
		schemas:                  make(map[string]*tableSchema),
		enumMappings:             make(map[string]map[int]enumDict),
		enumDefaultValueMappings: make(map[string]map[int]int),
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
	upsertBatchBuilder := memCom.NewUpsertBatchBuilder()
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

	// use abandonRows to record abandoned row index due to invalid data
	abandonRows := map[int]interface{}{}

	for colIndex, columnName := range columnNames {
		columnID, exist := schema.ColumnDict[columnName]
		if !exist {
			continue
		}
		column := schema.Table.Columns[columnID]

		// following conditions only overwrite is supported:
		// 1. dimension table (TODO: might support min/max in the future if needed)
		// 2. primary key column
		// 3. archiving sort column
		// 4. data type not in uint8, int8, uint16, int16, uint32, int32, float32
		if (!schema.Table.IsFactTable ||
			utils.IndexOfInt(schema.Table.PrimaryKeyColumns, columnID) >= 0 ||
			utils.IndexOfInt(schema.Table.ArchivingSortColumns, columnID) >= 0 ||
			schema.Table.Columns[columnID].IsOverwriteOnlyDataType()) &&
			updateModes[colIndex] > memCom.UpdateForceOverwrite {
			return nil, 0, utils.StackError(nil, "column %s only supports overwrite", columnName)
		}

		dataType := memCom.DataTypeForColumn(column)
		if err = upsertBatchBuilder.AddColumnWithUpdateMode(columnID, dataType, updateModes[colIndex]); err != nil {
			return nil, 0, err
		}
	}

	missingEnumCasesByColumnIDs := make(map[int]map[string]struct{})
	for rowIndex, row := range rows {
		if _, exist := abandonRows[rowIndex]; exist {
			continue
		}
		upsertBatchBuilder.AddRow()

		upsertBatchColumnIndex := 0
		for inputColIndex, columnName := range columnNames {
			columnID, exist := schema.ColumnDict[columnName]
			if !exist {
				continue
			}
			column := schema.Table.Columns[columnID]

			value := row[inputColIndex]

			// prevent primary key being nil
			if value == nil && utils.IndexOfInt(schema.Table.PrimaryKeyColumns, columnID) >= 0 {
				upsertBatchBuilder.RemoveRow()
				c.logger.With(
					"name", "prepareUpsertBatch",
					"table", tableName,
					"columnID", columnID,
					"value", value).Error("PrimaryKey column is nil")
				break
			}

			// skip rows if time column is nil for fact table
			if value == nil && schema.Table.IsFactTable && !schema.Table.Config.AllowMissingEventTime && columnID == 0 {
				upsertBatchBuilder.RemoveRow()
				c.logger.With(
					"name", "prepareUpsertBatch",
					"table", tableName,
					"columnID", columnID,
					"value", value).Error("Time column is nil")
				break
			}

			if column.IsEnumColumn() && value != nil {
				enumCase, isStr := value.(string)
				if !isStr {
					upsertBatchBuilder.RemoveRow()
					c.logger.With("name", "prepareUpsertBatch", "error", "enum value is not string", "table", tableName, "columnID", columnID, "value", value).Error("Failed to set value")
					break
				}
				if column.CaseInsensitive {
					enumCase = strings.ToLower(enumCase)
				}

				value = enumCase
				// only translate enum case to enum id when auto expand for column is disabled
				if column.DisableAutoExpand {
					value, err = c.translateEnum(tableName, columnID, enumCase, missingEnumCasesByColumnIDs)
					if err != nil {
						upsertBatchBuilder.RemoveRow()
						c.logger.With(
							"name", "prepareUpsertBatch",
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
					c.logger.With("name", "prepareUpsertBatch", "error", err.Error(), "table", tableName, "columnID", columnID, "value", value).Error("Failed to set value")
					break
				}
				if err = upsertBatchBuilder.SetValue(upsertBatchBuilder.NumRows-1, upsertBatchColumnIndex, value); err != nil {
					upsertBatchBuilder.RemoveRow()
					c.logger.With("name", "prepareUpsertBatch", "error", err.Error(), "table", tableName, "columnID", columnID, "value", value).Error("Failed to set value")
					break
				}
			} else {
				// directly insert value
				if err = upsertBatchBuilder.SetValue(upsertBatchBuilder.NumRows-1, upsertBatchColumnIndex, value); err != nil {
					upsertBatchBuilder.RemoveRow()
					c.logger.With("name", "prepareUpsertBatch", "error", err.Error(), "table", tableName, "columnID", columnID, "value", value).Error("Failed to set value")
					break
				}
			}
			upsertBatchColumnIndex++
		}
	}
	c.reportMissingEnums(schema, missingEnumCasesByColumnIDs)

	batchBytes, err := upsertBatchBuilder.ToByteArray()
	return batchBytes, upsertBatchBuilder.NumRows, err
}

func (c *connector) reportMissingEnums(schema *tableSchema, missingEnumCasesByColumnIDs map[int]map[string]struct{}) {
	for columnID, missingCasesSet := range missingEnumCasesByColumnIDs {
		if len(missingCasesSet) > 0 {
			missingCasesSlice := make([]string, 0, len(missingCasesSet))
			for c := range missingCasesSet {
				missingCasesSlice = append(missingCasesSlice, c)
			}
			// It's recommended to set up elk or sentry logging to catch this error.
			c.logger.With(
				"TableName", schema.Table.Name,
				"ColumnName", schema.Table.Columns[columnID],
				"ColumnID", columnID,
				"newEnumCasesSet", missingCasesSlice,
				"caseInsensitive", schema.Table.Columns[columnID].CaseInsensitive,
			).Error("Finding new enum cases during ingestion but enum auto expansion is disabled")
			c.metricScope.Tagged(
				map[string]string{
					"TableName": schema.Table.Name,
					"ColumnID":  strconv.Itoa(columnID),
				},
			).Counter("new_enum_cases_ignored").Inc(int64(len(missingCasesSlice)))
		}
	}
}

// checkPrimaryKeys checks whether primary key is missing
func (c *connector) checkPrimaryKeys(schema *tableSchema, columnNames []string) error {
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
func (c *connector) checkTimeColumnExistence(schema *tableSchema, columnNames []string) error {
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

func (c *connector) translateEnum(tableName string, columnID int, enumCase string, missingEnumCasesByColumnIDs map[int]map[string]struct{}) (enumID int, err error) {
	c.RLock()
	// here it already make sure the enum dictionary exists in cache
	enumID, ok := c.enumMappings[tableName][columnID][enumCase]
	c.RUnlock()
	if !ok {
		c.metricScope.Tagged(
			map[string]string{
				"TableName": tableName,
				"ColumnID":  strconv.Itoa(columnID),
			},
		).Counter("new_enum_case_rows_ignored").Inc(int64(1))
		if _, ok := missingEnumCasesByColumnIDs[columnID]; !ok {
			missingEnumCasesByColumnIDs[columnID] = make(map[string]struct{})
		}
		missingEnumCasesByColumnIDs[columnID][enumCase] = struct{}{}
		if defaultValue, ok := c.enumDefaultValueMappings[tableName][columnID]; ok {
			return defaultValue, nil
		}
		return -1, nil
	}
	return enumID, nil
}

func (c *connector) getTableSchema(tableName string) (*tableSchema, error) {
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

func (c *connector) fetchEnumCases(tableName, columnName string) ([]string, error) {
	var enumDictReponse []string

	resp, err := c.httpClient.Get(c.enumDictPath(tableName, columnName))

	err = c.readJSONResponse(resp, err, &enumDictReponse)

	return enumDictReponse, err
}

func (c *connector) fetchTableSchema(tableName string) (*tableSchema, error) {
	var table metaCom.Table

	resp, err := c.httpClient.Get(c.tablePath(tableName))
	err = c.readJSONResponse(resp, err, &table)
	if err != nil {
		return nil, err
	}

	columnDict := make(map[string]int)
	for columnID, column := range table.Columns {
		if !column.Deleted {
			columnDict[column.Name] = columnID
		}
	}

	schema := &tableSchema{
		Table:      &table,
		ColumnDict: columnDict,
	}

	c.Lock()
	defer c.Unlock()

	c.schemas[tableName] = schema
	// pre-create all enum mappings
	if _, tableExist := c.enumMappings[tableName]; !tableExist {
		c.enumMappings[tableName] = make(map[int]enumDict)
		c.enumDefaultValueMappings[tableName] = make(map[int]int)
	}
	for columnID, column := range table.Columns {
		if !column.Deleted && column.IsEnumColumn() &&
			// only fetch enum dict for column with auto expand disabled
			// for those enums will be provided by user
			// when auto expansion is not disabled, we just send enum case directly
			column.DisableAutoExpand {
			if _, columnExist := c.enumMappings[tableName][columnID]; !columnExist {
				c.enumMappings[tableName][columnID] = make(enumDict)
				// fetch enum cases for new column
				err = c.fetchAndSetEnumCasesWithLock(tableName, column.Name, columnID, column.CaseInsensitive, column.DefaultValue)
				if err != nil {
					return schema, err
				}
			}
		}
	}
	return schema, err
}

func (c *connector) fetchAndSetEnumCasesWithLock(tableName, columnName string, columnID int, caseInsensitive bool, defValuePtr *string) error {
	if defValuePtr != nil {
		defValue := *defValuePtr
		if caseInsensitive {
			defValue = strings.ToLower(defValue)
		}
		defValuePtr = &defValue
	}
	enumCases, err := c.fetchEnumCases(tableName, columnName)
	if err != nil {
		c.metricScope.Tagged(map[string]string{
			"table":    tableName,
			"columnID": strconv.Itoa(columnID),
		}).Counter("err_fetch_enum_dict").Inc(1)
		return utils.StackError(err, "Failed to fetch enum cases for table: %s, column: %d", tableName, columnID)
	}
	for enumID, enumCase := range enumCases {
		// Convert to lower case for comparison during ingestion.
		if caseInsensitive {
			enumCase = strings.ToLower(enumCase)
		}
		// all mapping should be pre created
		c.enumMappings[tableName][columnID][enumCase] = enumID
		if defValuePtr != nil {
			if *defValuePtr == enumCase {
				c.enumDefaultValueMappings[tableName][columnID] = enumID
			}
		}
	}
	return nil
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

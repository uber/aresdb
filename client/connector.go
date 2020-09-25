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
	"fmt"
	"net/http"
	"sync"
	"time"

	"strconv"
	"strings"
	"unsafe"

	"encoding/json"
	"github.com/uber-go/tally"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

const (
	// default request time out in seconds
	defaultRequestTimeout = 120
	// default schema refresh interval in seconds
	defaultSchemaRefreshInterval = 600
	dataIngestionHeader          = "application/upsert-data"
	applicationJSONHeader        = "application/json"
	defaultStringEnumLength      = 1024
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
	// Close the connection
	Close()
}

// UpsertBatchBuilder is an interface of upsertBatch on client side
type UpsertBatchBuilder interface {
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

	logger        *zap.SugaredLogger
	metricScope   tally.Scope
	schemaHandler *CachedSchemaHandler
}

// connector is the ares connector implementation
type connector struct {
	cfg                ConnectorConfig
	httpClient         http.Client
	upsertBatchBuilder UpsertBatchBuilder
	schemaHandler      *CachedSchemaHandler
}

// ConnectorConfig holds the configurations for ares Connector.
type ConnectorConfig struct {
	// Address is in the format of host:port
	Address string `yaml:"address" json:"address"`
	// DeviceChoosingTimeout value is the request timeout in seconds for http calls
	// if <= 0, will use default
	Timeout int `yaml:"timeout" json:"timeout"`
	// SchemaRefreshInterval is the interval in seconds for the connector to
	// fetch and refresh schema from ares
	// if <= 0, will use default
	SchemaRefreshInterval int `yaml:"schemaRefreshInterval" json:"schemaRefreshInterval"`
}

func NewUpsertBatchBuilderImpl(logger *zap.SugaredLogger, scope tally.Scope, schemaHandler *CachedSchemaHandler) UpsertBatchBuilder {
	return &UpsertBatchBuilderImpl{
		logger:        logger,
		metricScope:   scope,
		schemaHandler: schemaHandler,
	}
}

// NewConnector returns a new ares Connector
func (cfg ConnectorConfig) NewConnector(logger *zap.SugaredLogger, metricScope tally.Scope) Connector {
	if cfg.SchemaRefreshInterval <= 0 {
		cfg.SchemaRefreshInterval = defaultSchemaRefreshInterval
	}

	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultRequestTimeout
	}

	httpClient := http.Client{
		Timeout: time.Duration(cfg.Timeout) * time.Second,
	}

	httpSchemaFetcher := NewHttpSchemaFetcher(httpClient, cfg.Address, metricScope)
	cachedSchemaHandler := NewCachedSchemaHandler(logger, metricScope, httpSchemaFetcher)
	cachedSchemaHandler.Start(cfg.SchemaRefreshInterval)

	connector := &connector{
		cfg:        cfg,
		httpClient: httpClient,
		upsertBatchBuilder: &UpsertBatchBuilderImpl{
			logger:        logger,
			metricScope:   metricScope,
			schemaHandler: cachedSchemaHandler,
		},
		schemaHandler: cachedSchemaHandler,
	}
	return connector
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

// Close the connection
func (c *connector) Close() {
	c.schemaHandler = nil
	c.upsertBatchBuilder = nil
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
	schema, err := c.schemaHandler.FetchSchema(tableName)
	if err != nil {
		return nil, 0, err
	}

	// return error if primary key is missing
	if err = checkPrimaryKeys(schema, columnNames); err != nil {
		return nil, 0, err
	}

	// return error if time column is missing
	if err = checkTimeColumnExistence(schema, columnNames); err != nil {
		return nil, 0, err
	}

	return c.upsertBatchBuilder.PrepareUpsertBatch(tableName, columnNames, updateModes, rows)
}

// checkPrimaryKeys checks whether primary key is missing
func checkPrimaryKeys(schema *TableSchema, columnNames []string) error {
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
func checkTimeColumnExistence(schema *TableSchema, columnNames []string) error {
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

func (c *connector) dataPath(tableName string, shard int) string {
	return fmt.Sprintf("http://%s/data/%s/%d", c.cfg.Address, tableName, shard)
}

func (u *UpsertBatchBuilderImpl) prepareEnumCases(isEnumArrayCol bool, tableName, columnName string, colIndex, columnID int, rows []Row, abandonRows map[int]struct{}, caseInsensitive bool, disableAutoExpand bool) error {
	enumCaseSet := make(map[string]struct{})
	for rowIndex, row := range rows {
		if _, exist := abandonRows[rowIndex]; exist {
			continue
		}
		value := row[colIndex]

		if value == nil {
			continue
		}

		abandonRow := false
		if enumCase, ok := value.(string); ok {
			if len(enumCase) > defaultStringEnumLength {
				abandonRow = true
				u.logger.With(
					"name", "prepareEnumCases",
					"error", "Enum string value is too long",
					"table", tableName,
					"columnID", columnID,
					"value", value).Debug("Enum string value is too long")
				u.metricScope.Tagged(map[string]string{"table": tableName, "columnID": strconv.Itoa(columnID)}).
					Counter("abandoned_rows_long_string").Inc(1)
			} else {
				if isEnumArrayCol {
					arrVal := make([]interface{}, 0)
					if err := json.Unmarshal([]byte(enumCase), &arrVal); err != nil {
						abandonRow = true
						u.logger.With(
							"name", "prepareEnumCases",
							"error", "Fail to unmarshal enum array value",
							"table", tableName,
							"columnID", columnID,
							"value", value).Debug("Fail to unmarshal enum array value")
						u.metricScope.Tagged(map[string]string{"table": tableName, "columnID": strconv.Itoa(columnID)}).
							Counter("abandoned_rows_enum_array_parse_error").Inc(1)
					} else {
						for _, val := range arrVal {
							if val != nil {
								item, ok := val.(string)
								if !ok {
									abandonRow = true
									u.logger.With(
										"name", "prepareEnumCases",
										"error", "Enum array value should be string",
										"table", tableName,
										"columnID", columnID,
										"value", value).Debug("Enum array value is not string")
									break
								}
								if caseInsensitive {
									item = strings.ToLower(item)
								}
								enumCaseSet[item] = struct{}{}
							}
						}
					}
				} else {
					if caseInsensitive {
						enumCase = strings.ToLower(enumCase)
					}
					enumCaseSet[enumCase] = struct{}{}
				}
			}
		} else {
			abandonRow = true
			u.logger.With(
				"name", "prepareEnumCases",
				"error", "Enum value should be string",
				"table", tableName,
				"columnID", columnID,
				"value", value).Debug("Enum value is not string")

		}
		if abandonRow {
			u.metricScope.Tagged(map[string]string{"table": tableName, "columnID": strconv.Itoa(columnID)}).
				Counter("abandoned_rows").Inc(1)
			abandonRows[rowIndex] = struct{}{}
		}
	}

	if len(enumCaseSet) > 0 {
		enumCases := make([]string, 0, len(enumCaseSet))
		for enumCase := range enumCaseSet {
			enumCases = append(enumCases, enumCase)
		}
		err := u.schemaHandler.PrepareEnumCases(tableName, columnName, enumCases)
		if err != nil {
			return err
		}
	}
	return nil
}

// PrepareUpsertBatch prepares the upsert batch for upsert,
// returns upsertBatch byte array, number of rows in upsert batch and error.
func (u *UpsertBatchBuilderImpl) PrepareUpsertBatch(tableName string, columnNames []string,
	updateModes []memCom.ColumnUpdateMode, rows []Row) ([]byte, int, error) {
	var err error
	upsertBatchBuilder := memCom.NewUpsertBatchBuilder()

	schema, err := u.schemaHandler.FetchSchema(tableName)
	if err != nil {
		return nil, 0, err
	}

	if schema.Table.IsFactTable {
		upsertBatchBuilder.MarkFactTable()
	}

	// use abandonRows to record abandoned row index due to invalid data
	abandonRows := make(map[int]struct{})

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

		if column.IsEnumBasedColumn() {
			if err = u.prepareEnumCases(column.IsEnumArrayColumn(), tableName, columnName, colIndex, columnID, rows, abandonRows, column.CaseInsensitive, column.DisableAutoExpand); err != nil {
				return nil, 0, err
			}
		}
	}

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
				u.logger.With(
					"name", "PrepareUpsertBatch",
					"table", tableName,
					"columnID", columnID,
					"value", value).Error("PrimaryKey column is nil")
				break
			}

			// skip rows if time column is nil for fact table
			if value == nil && schema.Table.IsFactTable && !schema.Table.Config.AllowMissingEventTime && columnID == 0 {
				upsertBatchBuilder.RemoveRow()
				u.logger.With(
					"name", "PrepareUpsertBatch",
					"table", tableName,
					"columnID", columnID,
					"value", value).Error("Time column is nil")
				break
			}

			if column.IsEnumBasedColumn() {
				if column.IsEnumArrayColumn() {
					if value != nil {
						arrVal := make([]interface{}, 0)
						// no error handling here as it should already be covered in prepareEnumCases
						json.Unmarshal([]byte(value.(string)), &arrVal)
						hasError := false
						for i := 0; i < len(arrVal); i++ {
							arrVal[i], err = u.schemaHandler.TranslateEnum(tableName, columnID, arrVal[i], column.CaseInsensitive)
							if err != nil {
								hasError = true
								upsertBatchBuilder.RemoveRow()
								u.logger.With(
									"name", "prepareUpsertBatch",
									"error", err.Error(),
									"table", tableName,
									"columnID", columnID,
									"value", arrVal[i]).Error("Failed to translate enum")
								break
							}
							if arrVal[i] == -1 {
								arrVal[i] = nil
							}
						}
						if hasError {
							break
						}
						value = arrVal
					}
				} else {
					value, err = u.schemaHandler.TranslateEnum(tableName, columnID, value, column.CaseInsensitive)
					if err != nil {
						upsertBatchBuilder.RemoveRow()
						u.logger.With(
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



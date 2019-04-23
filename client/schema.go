package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uber-go/tally"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// SchemaFetcher is the interface for fetch schema and enums
type SchemaFetcher interface {
	// FetchAllSchemas fetches all schemas
	FetchAllSchemas() ([]metaCom.Table, error)
	// FetchSchema fetch one schema for given table
	FetchSchema(table string) (*metaCom.Table, error)
	// FetchAllEnums fetches all enums for given table and column
	FetchAllEnums(tableName string, columnName string) ([]string, error)
	// ExtendEnumCases extends enum cases to given table column
	ExtendEnumCases(tableName, columnName string, enumCases []string) ([]int, error)
}

// httpSchemaFetcher is a http based schema fetcher
type httpSchemaFetcher struct {
	httpClient  http.Client
	metricScope tally.Scope
	address     string
}

// CachedSchemaHandler handles schema and enum requests with cache
type CachedSchemaHandler struct {
	*sync.RWMutex

	logger        *zap.SugaredLogger
	metricScope   tally.Scope
	schemaFetcher SchemaFetcher

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

// NewCachedSchemaHandler creates a new cached schema handler
func NewCachedSchemaHandler(logger *zap.SugaredLogger, scope tally.Scope, schamaFetcher SchemaFetcher) *CachedSchemaHandler {
	return &CachedSchemaHandler{
		RWMutex:                  &sync.RWMutex{},
		logger:                   logger,
		metricScope:              scope,
		schemaFetcher:            schamaFetcher,
		schemas:                  make(map[string]*TableSchema),
		enumMappings:             make(map[string]map[int]enumDict),
		enumDefaultValueMappings: make(map[string]map[int]int),
	}
}

// NewHttpSchemaFetcher creates a new http schema fetcher
func NewHttpSchemaFetcher(httpClient http.Client, address string, scope tally.Scope) SchemaFetcher {
	return &httpSchemaFetcher{
		metricScope: scope,
		address:     address,
		httpClient:  httpClient,
	}
}

// Start starts the CachedSchemaHandler, if interval > 0, will start periodical refresh
func (cf *CachedSchemaHandler) Start(interval int) error {
	err := cf.FetchAllSchema()
	if err != nil {
		return err
	}

	if interval <= 0 {
		return nil
	}

	go func(refreshInterval int) {
		ticks := time.Tick(time.Duration(refreshInterval) * time.Second)
		for range ticks {
			err = cf.FetchAllSchema()
			if err != nil {
				cf.logger.With(
					"error", err.Error()).Errorf("Failed to fetch table schema")
			}
		}
	}(interval)
	return nil
}

// TranslateEnum translates given enum value to its enumID
func (cf *CachedSchemaHandler) TranslateEnum(tableName string, columnID int, value interface{}, caseInsensitive bool) (enumID int, err error) {
	if value == nil {
		return -1, nil
	}
	enumCase, ok := value.(string)
	if !ok {
		return 0, utils.StackError(nil, "Enum value should be string, but got: %T", value)
	}
	if caseInsensitive {
		enumCase = strings.ToLower(enumCase)
	}
	cf.RLock()
	// here it already make sure the enum dictionary exists in cache
	enumID, ok = cf.enumMappings[tableName][columnID][enumCase]
	cf.RUnlock()
	if !ok {
		cf.metricScope.Tagged(
			map[string]string{
				"TableName": tableName,
				"ColumnID":  strconv.Itoa(columnID),
			},
		).Counter("new_enum_case_rows_ignored").Inc(int64(1))
		if defaultValue, ok := cf.enumDefaultValueMappings[tableName][columnID]; ok {
			return defaultValue, nil
		}
		return -1, nil
	}
	return enumID, nil
}

// FetchAllSchema fetch all schemas
func (cf *CachedSchemaHandler) FetchAllSchema() error {
	tables, err := cf.schemaFetcher.FetchAllSchemas()
	if err != nil {
		return err
	}

	for _, table := range tables {
		cf.setTable(&table)
		err := cf.fetchAndSetEnumCases(&table)
		if err != nil {
			return err
		}
	}
	return nil
}

// FetchSchema fetchs the schema of given table name
func (cf *CachedSchemaHandler) FetchSchema(tableName string) (*TableSchema, error) {
	cf.RLock()
	schema, exist := cf.schemas[tableName]
	cf.RUnlock()
	if exist {
		return schema, nil
	}
	table, err := cf.schemaFetcher.FetchSchema(tableName)
	if err != nil {
		return nil, err
	}
	schema = cf.setTable(table)
	err = cf.fetchAndSetEnumCases(table)
	return schema, err
}

// PrepareEnumCases prepares enum cases
func (cf *CachedSchemaHandler) PrepareEnumCases(tableName, columnName string, enumCases []string) error {
	newEnumCases := make([]string, 0, len(enumCases))
	cf.RLock()
	schema, exist := cf.schemas[tableName]
	if !exist {
		cf.RUnlock()
		return nil
	}
	columnID, exist := schema.ColumnDict[columnName]
	if !exist {
		cf.RUnlock()
		return nil
	}

	caseInsensitive := schema.Table.Columns[columnID].CaseInsensitive
	disableAutoExpand := schema.Table.Columns[columnID].DisableAutoExpand
	for _, enumCase := range enumCases {
		if _, exist := cf.enumMappings[tableName][columnID][enumCase]; !exist {
			newEnumCases = append(newEnumCases, enumCase)
		}
	}
	cf.RUnlock()

	if disableAutoExpand {
		// It's recommended to set up elk or sentry logging to catch this error.
		cf.logger.With(
			"TableName", tableName,
			"ColumnName", columnName,
			"ColumnID", columnID,
			"newEnumCasesSet", newEnumCases,
			"caseInsensitive", caseInsensitive,
		).Error("Finding new enum cases during ingestion but enum auto expansion is disabled")
		cf.metricScope.Tagged(
			map[string]string{
				"TableName": tableName,
				"ColumnID":  strconv.Itoa(columnID),
			},
		).Counter("new_enum_cases_ignored").Inc(int64(len(newEnumCases)))
		return nil
	}

	enumIDs, err := cf.schemaFetcher.ExtendEnumCases(tableName, columnName, newEnumCases)
	if err != nil {
		return err
	}

	cf.Lock()
	for index, enumCase := range newEnumCases {
		if caseInsensitive {
			enumCase = strings.ToLower(enumCase)
		}
		cf.enumMappings[tableName][columnID][enumCase] = enumIDs[index]
	}
	cf.Unlock()
	return nil
}

func (cf *CachedSchemaHandler) fetchAndSetEnumCases(table *metaCom.Table) error {
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

		if column.IsEnumColumn() {
			enumCases, err := cf.schemaFetcher.FetchAllEnums(table.Name, column.Name)
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
				cf.metricScope.Tagged(map[string]string{
					"table":    table.Name,
					"columnID": strconv.Itoa(columnID),
				}).Counter("err_fetch_enum_dict").Inc(1)
				return utils.StackError(err, "Failed to fetch enum cases for table: %s, column: %d", table.Name, columnID)
			}
		}
	}
	cf.Lock()
	cf.enumMappings[table.Name] = enumMappings
	cf.enumDefaultValueMappings[table.Name] = enumDefaultValueMappings
	cf.Unlock()
	return nil
}

func (cf *CachedSchemaHandler) setTable(table *metaCom.Table) *TableSchema {
	columnDict := make(map[string]int)
	for columnID, column := range table.Columns {
		if !column.Deleted {
			columnDict[column.Name] = columnID
		}
	}

	schema := &TableSchema{
		Table:      table,
		ColumnDict: columnDict,
	}

	cf.Lock()
	cf.schemas[table.Name] = schema
	if _, tableExist := cf.enumMappings[table.Name]; !tableExist {
		cf.enumMappings[table.Name] = make(map[int]enumDict)
		cf.enumDefaultValueMappings[table.Name] = make(map[int]int)
	}
	for columnID, column := range table.Columns {
		if !column.Deleted && column.IsEnumColumn() {
			if _, columnExist := cf.enumMappings[table.Name][columnID]; !columnExist {
				cf.enumMappings[table.Name][columnID] = make(enumDict)
			}
		}
	}
	cf.Unlock()
	return schema
}

func (hf *httpSchemaFetcher) FetchAllEnums(tableName, columnName string) ([]string, error) {
	var enumDictReponse []string

	resp, err := hf.httpClient.Get(hf.enumDictPath(tableName, columnName))

	err = hf.readJSONResponse(resp, err, &enumDictReponse)

	return enumDictReponse, err
}

func (hf *httpSchemaFetcher) ExtendEnumCases(tableName, columnName string, enumCases []string) ([]int, error) {
	enumCasesRequest := EnumCasesWrapper{
		EnumCases: enumCases,
	}

	enumCasesBytes, err := json.Marshal(enumCasesRequest)
	if err != nil {
		return nil, utils.StackError(err, "Failed to marshal enum cases")
	}

	var enumIDs []int
	resp, err := hf.httpClient.Post(hf.enumDictPath(tableName, columnName), applicationJSONHeader, bytes.NewReader(enumCasesBytes))
	err = hf.readJSONResponse(resp, err, &enumIDs)
	if err != nil {
		return nil, err
	}
	return enumIDs, nil
}

func (hf *httpSchemaFetcher) FetchSchema(tableName string) (*metaCom.Table, error) {
	var table metaCom.Table
	resp, err := hf.httpClient.Get(hf.tablePath(tableName))
	err = hf.readJSONResponse(resp, err, &table)
	if err != nil {
		return nil, err
	}

	return &table, nil
}

func (hf *httpSchemaFetcher) FetchAllSchemas() ([]metaCom.Table, error) {
	var tables []string
	resp, err := hf.httpClient.Get(hf.listTablesPath())
	err = hf.readJSONResponse(resp, err, &tables)
	if err != nil {
		return nil, utils.StackError(err, "Failed to fetch table list")
	}

	var res []metaCom.Table
	for _, tableName := range tables {
		table, err := hf.FetchSchema(tableName)
		if err != nil {
			hf.metricScope.Tagged(map[string]string{
				"table": tableName,
			}).Counter("err_fetch_table").Inc(1)
			return nil, utils.StackError(err, "Failed to fetch schema error")
		}
		res = append(res, *table)
	}

	return res, nil
}

func (hf *httpSchemaFetcher) readJSONResponse(response *http.Response, err error, data interface{}) error {
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

func (hf *httpSchemaFetcher) tablePath(tableName string) string {
	return fmt.Sprintf("%s/%s", hf.listTablesPath(), tableName)
}

func (hf *httpSchemaFetcher) listTablesPath() string {
	return fmt.Sprintf("http://%s/schema/tables", hf.address)
}

func (hf *httpSchemaFetcher) enumDictPath(tableName, columnName string) string {
	return fmt.Sprintf("%s/%s/columns/%s/enum-cases", hf.listTablesPath(), tableName, columnName)
}

package utils

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strings"

	"bytes"
	"encoding/json"
	"github.com/gofrs/uuid"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"strconv"
	"time"
)

var (
	random        = rand.New(rand.NewSource(0))
	unitToSeconds = map[string]time.Duration{
		"m": time.Minute,
		"h": time.Hour,
		"d": 24 * time.Hour,
	}

	logger = zap.NewExample().Sugar()
)

type TableCreationResp struct {
	Message string `json:"message"`
	Cause   string `json:"cause"`
}

func parseAndGenerateRandomTime(timeStr string) uint32 {
	timeStr = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(timeStr, "{"), "}"))

	t, err := strconv.Atoi(timeStr[:len(timeStr)-1])
	PanicIfErr(err)

	unit, ok := unitToSeconds[timeStr[len(timeStr)-1:]]
	if !ok {
		panic(fmt.Sprintf("unit %s not supported", timeStr[len(timeStr)-1:]))
	}

	duration := time.Duration(t) * unit
	start := utils.Now().Add(-duration)

	return uint32(start.Unix() + random.Int63n(int64(duration.Seconds())))
}

func PanicIfErr(err error) {
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		panic(err)
	}
}

func logError(msg string) {
	os.Stderr.WriteString(msg + "\n")
}

func ingestDataForArrayTestTable(connector client.Connector, tableName string, dataFilePath string) {
	file, err := os.Open(dataFilePath)
	PanicIfErr(err)
	defer file.Close()

	csvReader := csv.NewReader(file)
	columnNames, err := csvReader.Read()
	PanicIfErr(err)

	var record []string
	batches := 2
	batchRows := 1000
	rowsInserted := 0

	for record, err = csvReader.Read(); err != io.EOF; record, err = csvReader.Read() {
		PanicIfErr(err)
		for i := 0; i < batches; i++ {
			rows := make([]client.Row, batchRows)
			for j := 0; j < batchRows; j++ {
				row := make(client.Row, len(record))
				for k, value := range record {
					row[k] = generateArrayTableColValue(value, j%5)
				}
				rows[j] = row
			}
			n, err := connector.Insert(tableName, columnNames, rows)
			PanicIfErr(err)
			rowsInserted += n
		}
	}
	fmt.Printf("Total %d rows data inserted into table: %s\n", rowsInserted, tableName)
}

func generateArrayTableColValue(val string, arraySize int) interface{} {
	if strings.HasPrefix(val, "{time-") && strings.HasSuffix(val, "}") {
		return parseAndGenerateRandomTime("{" + val[6:])
	}
	switch val {
	case "{uuid}":
		val, _ := uuid.NewV4()
		return val.String()
	case "{uint16}":
		return arraySize
	case "{uint8}":
		return arraySize
	case "{smallenum}":
		return "status_" + strconv.Itoa(arraySize)
	case "{float32}":
		return 1.01 * float32(arraySize)
	default:
		if strings.HasPrefix(val, "{array_") && strings.HasSuffix(val, "}") {
			return generateArrayValue(val[7:len(val)-1], arraySize)
		}
	}
	return nil
}

func generateArrayValue(valType string, arraySize int) interface{} {
	if arraySize == 0 {
		return nil
	}
	res := "["
	for i := 1; i < arraySize; i++ {
		if i != 1 {
			res += ","
		}
		res += generateArrayItemValue(valType, i)
	}
	res += "]"
	return res
}

func generateArrayItemValue(valType string, itemNo int) string {
	if itemNo == 3 {
		return "null"
	}
	switch valType {
	case "bool":
		if itemNo%2 == 0 {
			return "\"true\""
		}
		return "\"false\""
	case "int8", "int16", "int32", "uint8", "uint16", "uint32":
		return "\"" + strconv.Itoa(itemNo*10) + "\""
	case "smallenum":
		return "\"enum_value_" + strconv.Itoa(itemNo) + "\""
	case "bigenum":
		return "\"enum_value_" + strconv.Itoa(itemNo*10) + "\""
	case "uuid":
		return "\"12000000-0000-0000-0100-" + fmt.Sprintf("%012d", itemNo*10) + "\""
	case "geopoint":
		val := fmt.Sprintf("\"point(-%.6f %.6f)\"", float32(itemNo*10), float32(itemNo*10))
		return val
	default:
		return ""
	}
}

func MakeQuery(host string, port int, queryName, queryType, queryPath string) string {
	var err error
	var file *os.File
	file, err = os.Open(queryPath)
	PanicIfErr(err)

	fmt.Printf("making query %s ... \n", queryName)
	var resp *http.Response
	if queryType == ".aql" {
		resp, err = http.Post(fmt.Sprintf("http://%s:%d/query/aql", host, port), "application/json", file)
	} else if queryType == ".sql" {
		resp, err = http.Post(fmt.Sprintf("http://%s:%d/query/sql", host, port), "application/json", file)
	} else {
		err = fmt.Errorf("query file has to have .aql or .sql suffix")
	}
	PanicIfErr(err)
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Errorf("query failed with status code %d", resp.StatusCode))
	}

	result, err := ioutil.ReadAll(resp.Body)
	PanicIfErr(err)

	prettJSON := &bytes.Buffer{}
	PanicIfErr(json.Indent(prettJSON, result, "", "\t"))

	fmt.Println(prettJSON.String())
	return prettJSON.String()
}

func CreateTable(host string, port int, tableName string, tableSchemaPath string) {
	schemaFile, err := os.Open(tableSchemaPath)
	PanicIfErr(err)

	schemaCreationURL := fmt.Sprintf("http://%s/schema/tables", fmt.Sprintf("%s:%d", host, port))
	rsp, err := http.Post(schemaCreationURL, "application/json", schemaFile)
	PanicIfErr(err)

	if rsp.Body != nil {
		defer rsp.Body.Close()
	}

	if rsp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("schema creation failed with status code %d\n", rsp.StatusCode)
		fmt.Println(msg)
		if rsp.Body != nil {
			respBody, err := ioutil.ReadAll(rsp.Body)
			PanicIfErr(err)
			// TODO(lucafuji): need a better error code mapping here to tell it's a table exists error.
			var tableCreationResp TableCreationResp
			err = json.Unmarshal(respBody, &tableCreationResp)
			PanicIfErr(err)
			if tableCreationResp.Message == "Table already exists" {
				logError(fmt.Sprintf("Table %s already exists", tableName))
				return
			}
		}
		panic(msg)
	}
	fmt.Printf("table %s created\n", tableName)
}

func IngestDataForTable(host string, port int, tableName string, dataPath string) {
	cfg := client.ConnectorConfig{
		Address: fmt.Sprintf("%s:%d", host, port),
	}
	connector := cfg.NewConnector(logger, tally.NoopScope)
	defer connector.Close()

	// TODO for some hard coded table for array test now
	if tableName == "arraytest" {
		ingestDataForArrayTestTable(connector, tableName, dataPath)
		return
	}

	file, err := os.Open(dataPath)
	PanicIfErr(err)
	defer file.Close()

	csvReader := csv.NewReader(file)
	columnNames, err := csvReader.Read()
	PanicIfErr(err)

	rows := make([]client.Row, 0)
	var record []string

	for record, err = csvReader.Read(); err != io.EOF; record, err = csvReader.Read() {
		PanicIfErr(err)
		row := make(client.Row, 0, len(record))
		for _, value := range record {
			if strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}") {
				row = append(row, parseAndGenerateRandomTime(value))
			} else {
				row = append(row, value)
			}
		}
		rows = append(rows, row)
	}
	rowsInserted, err := connector.Insert(tableName, columnNames, rows)
	PanicIfErr(err)

	fmt.Printf("%d rows inserted into %s\n", rowsInserted, tableName)
}

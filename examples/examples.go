package main

import (
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/common"
	"go.uber.org/zap"
	"encoding/csv"
	"os"
	"io"
	"fmt"
	"net/http"
	"io/ioutil"
)

const (
	hostPort = "localhost:19374"
	schemaDir = "schema"
	dataDir = "data"
)

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func createTablesForDataSet(dataSetName string) {
	dataSetSchemaDir := fmt.Sprintf("%s/%s", schemaDir, dataSetName)
	schemaDirInfo, err := ioutil.ReadDir(dataSetSchemaDir)
	panicIfErr(err)
	for _, schemaInfo := range schemaDirInfo {
		tableSchemaPath := fmt.Sprintf("%s/%s/%s", schemaDir, dataSetName, schemaInfo.Name())
		createTable(tableSchemaPath)
	}
}

func createTable(tableSchemaPath string) {
	schemaFile, err := os.Open(tableSchemaPath)
	panicIfErr(err)

	schemaCreationURL := fmt.Sprintf("http://%s/schemas", hostPort)
	rsp, err := http.Post(schemaCreationURL, "application/json", schemaFile)
	panicIfErr(err)

	if rsp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("schema creation failed with status code %d", rsp.StatusCode))
	}
}

func ingestDataForDataSet(dataSetName string) {
	dataFileDir := fmt.Sprintf("%s/%s", dataDir, dataSetName)
	dataFiles, err := ioutil.ReadDir(dataFileDir)
	panicIfErr(err)

	for _, dataFileInfo := range dataFiles {
		dataFilePath := fmt.Sprintf("%s/%s/%s", dataDir, dataSetName, dataFileInfo.Name())
		ingestDataForTable(dataFileInfo.Name(), dataFilePath)
	}
}

func ingestDataForTable(tableName string, dataPath string) {
	cfg := client.ConnectorConfig{
		Address: hostPort,
	}

	logger := zap.NewExample().Sugar()
	metrics := common.NewNoopMetrics()
	scope, closer, err := metrics.NewRootScope()
	panicIfErr(err)

	defer closer.Close()

	connector, err := cfg.NewConnector(logger, scope)
	panicIfErr(err)

	file, err := os.Open(dataPath)
	panicIfErr(err)
	defer file.Close()

	csvReader := csv.NewReader(file)
	columnNames, err := csvReader.Read()
	panicIfErr(err)

	rows := make([]client.Row, 0)
	var record []string

	for record, err = csvReader.Read(); err != io.EOF; record, err = csvReader.Read() {
		panicIfErr(err)
		row := make(client.Row, 0, len(record))
		for _, value := range record {
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	rowsInserted, err := connector.Insert(tableName, columnNames, rows)
	fmt.Printf("%d rows inserted into %s\n", rowsInserted, tableName)
}

func main() {
	createTablesForDataSet("1k_trips")
	ingestDataForDataSet("1k_trips")
}

package main

import (
	"github.com/uber/aresdb/client"
	"encoding/csv"
	"os"
	"io"
	"fmt"
	"net/http"
	"io/ioutil"
	"math/rand"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"github.com/uber-go/tally"
)

const (
	hostPort = "localhost:19374"
	schemaDir = "schema"
	dataDir = "data"
	randomTime = "{time}"
)

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func createTablesForDataSet(dataSetName string) {
	dataSetSchemaDir := fmt.Sprintf("%s/%s", dataSetName, schemaDir)
	schemaDirInfo, err := ioutil.ReadDir(dataSetSchemaDir)
	panicIfErr(err)
	for _, schemaInfo := range schemaDirInfo {
		tableSchemaPath := fmt.Sprintf("%s/%s/%s", dataSetName, schemaDir, schemaInfo.Name())
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
	dataFileDir := fmt.Sprintf("./%s/%s", dataSetName, dataDir)
	dataFiles, err := ioutil.ReadDir(dataFileDir)
	panicIfErr(err)

	cfg := client.ConnectorConfig{
		Address: hostPort,
	}
	connector, err := cfg.NewConnector(zap.NewExample().Sugar(), tally.NoopScope)
	panicIfErr(err)

	for _, dataFileInfo := range dataFiles {
		dataFilePath := fmt.Sprintf("./%s/%s/%s", dataSetName, dataDir, dataFileInfo.Name())
		ingestDataForTable(connector, dataFileInfo.Name(), dataFilePath)
	}
}

func ingestDataForTable(connector client.Connector, tableName string, dataPath string) {
	file, err := os.Open(dataPath)
	panicIfErr(err)
	defer file.Close()

	csvReader := csv.NewReader(file)
	columnNames, err := csvReader.Read()
	panicIfErr(err)

	rows := make([]client.Row, 0)
	var record []string

	now := utils.Now().Unix()
	r := rand.New(rand.NewSource(0))
	for record, err = csvReader.Read(); err != io.EOF; record, err = csvReader.Read() {
		panicIfErr(err)
		row := make(client.Row, 0, len(record))
		for _, value := range record {
			if value == randomTime {
				row = append(row, getRandomEpochSeconds(r, now - 86400, now))
			} else {
				row = append(row, value)
			}
		}
		rows = append(rows, row)
	}

	rowsInserted, err := connector.Insert(tableName, columnNames, rows)
	fmt.Printf("%d rows inserted into %s\n", rowsInserted, tableName)
}

func getRandomEpochSeconds(r *rand.Rand, start, end int64) uint32 {
	return uint32(start + r.Int63n(end - start))
}

func main() {
	createTablesForDataSet("1k_trips")
	ingestDataForDataSet("1k_trips")
}

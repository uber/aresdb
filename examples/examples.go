package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"bytes"
	"encoding/json"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"strconv"
	"time"
)

const (
	schemaDir  = "schema"
	queriesDir = "queries"
	dataDir    = "data"

	dataSetKeyName = "dataset"
	hostKeyName    = "host"
	portKeyName    = "port"
)

var (
	random        = rand.New(rand.NewSource(0))
	now           = utils.Now()
	unitToSeconds = map[string]time.Duration{
		"m": time.Minute,
		"h": time.Hour,
		"d": 24 * time.Hour,
	}

	logger = zap.NewExample().Sugar()
)

func parseAndGenerateRandomTime(timeStr string) uint32 {
	timeStr = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(timeStr, "{"), "}"))

	t, err := strconv.Atoi(timeStr[:len(timeStr)-1])
	panicIfErr(err)

	unit, ok := unitToSeconds[timeStr[len(timeStr)-1:]]
	if !ok {
		panic(fmt.Sprintf("unit %s not supported", timeStr[len(timeStr)-1:]))
	}

	duration := time.Duration(t) * unit
	start := now.Add(-duration)

	return uint32(start.Unix() + random.Int63n(int64(duration.Seconds())))
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func queryDataSet() {
	dataSetName := viper.GetString(dataSetKeyName)
	dataSetQueriesDir := fmt.Sprintf("%s/%s", dataSetName, queriesDir)
	queriesDirInfo, err := ioutil.ReadDir(dataSetQueriesDir)
	panicIfErr(err)
	for _, queryInfo := range queriesDirInfo {
		baseName := queryInfo.Name()
		queryName := strings.TrimSuffix(baseName, filepath.Ext(baseName))
		queryPath := fmt.Sprintf("%s/%s/%s", dataSetName, queriesDir, baseName)
		makeQuery(queryName, queryPath)
	}
}

func createTablesForDataSet() {
	dataSetName := viper.GetString(dataSetKeyName)
	dataSetSchemaDir := fmt.Sprintf("%s/%s", dataSetName, schemaDir)
	schemaDirInfo, err := ioutil.ReadDir(dataSetSchemaDir)
	panicIfErr(err)
	for _, schemaInfo := range schemaDirInfo {
		baseName := schemaInfo.Name()
		tableName := strings.TrimSuffix(baseName, filepath.Ext(baseName))
		tableSchemaPath := fmt.Sprintf("%s/%s/%s", dataSetName, schemaDir, baseName)
		createTable(tableName, tableSchemaPath)
	}
}

func ingestDataForDataSet() {
	dataSetName := viper.GetString(dataSetKeyName)
	dataFileDir := fmt.Sprintf("./%s/%s", dataSetName, dataDir)
	dataFiles, err := ioutil.ReadDir(dataFileDir)
	panicIfErr(err)

	cfg := client.ConnectorConfig{
		Address: fmt.Sprintf("%s:%d", viper.GetString(hostKeyName), viper.GetInt(portKeyName)),
	}

	connector, err := cfg.NewConnector(logger, tally.NoopScope)
	panicIfErr(err)

	for _, dataFileInfo := range dataFiles {
		baseName := dataFileInfo.Name()
		dataFilePath := fmt.Sprintf("./%s/%s/%s", dataSetName, dataDir, baseName)
		tableName := strings.TrimSuffix(baseName, filepath.Ext(baseName))
		ingestDataForTable(connector, tableName, dataFilePath)
	}
}

func makeQuery(queryName, queryPath string) {
	file, err := os.Open(queryPath)
	panicIfErr(err)

	fmt.Printf("making query %s ... \n", queryName)
	resp, err := http.Post(fmt.Sprintf("http://%s:%d/query/aql", viper.GetString(hostKeyName), viper.GetInt(portKeyName)), "application/json", file)
	panicIfErr(err)
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Errorf("query failed with status code %d", resp.StatusCode))
	}

	result, err := ioutil.ReadAll(resp.Body)
	panicIfErr(err)

	prettJSON := &bytes.Buffer{}
	panicIfErr(json.Indent(prettJSON, result, "", "\t"))

	fmt.Println(prettJSON.String())
}

func createTable(tableName string, tableSchemaPath string) {
	schemaFile, err := os.Open(tableSchemaPath)
	panicIfErr(err)

	schemaCreationURL := fmt.Sprintf("http://%s/schema/tables", fmt.Sprintf("%s:%d", viper.GetString(hostKeyName), viper.GetInt(portKeyName)))
	rsp, err := http.Post(schemaCreationURL, "application/json", schemaFile)
	panicIfErr(err)

	if rsp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("schema creation failed with status code %d", rsp.StatusCode))
	}
	fmt.Printf("table %s created\n", tableName)
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

	for record, err = csvReader.Read(); err != io.EOF; record, err = csvReader.Read() {
		panicIfErr(err)
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
	panicIfErr(err)

	fmt.Printf("%d rows inserted into %s\n", rowsInserted, tableName)
}

func main() {
	rootCmd := &cobra.Command{
		Use:     "examples",
		Short:   "AresDB Examples",
		Long:    `AresDB Examples Contains examples for interact with aresdb`,
		Example: `./examples help tables`,
	}
	rootCmd.PersistentFlags().String(dataSetKeyName, "1k_trips", "name for data set")
	rootCmd.PersistentFlags().String(hostKeyName, "localhost", "host of aresdb server")
	rootCmd.PersistentFlags().Int(portKeyName, 9374, "port of aresdb server")
	viper.SetDefault(dataSetKeyName, "1k_trips")
	viper.SetDefault(hostKeyName, "localhost")
	viper.SetDefault(portKeyName, 9374)
	viper.BindPFlags(rootCmd.PersistentFlags())

	dataCmd := &cobra.Command{
		Use: "data",
		Short:   "Ingest data for example dataset",
		Long:    `Ingest data for example dataset`,
		Example: `./examples data --dataset 1k_trips`,
		Run: func(cmd *cobra.Command, args []string) {
			ingestDataForDataSet()
		},
	}

	tableCmd := &cobra.Command{
		Use: "tables",
		Short:   `Create tables for example dataset`,
		Long:    `Create tables for example dataset`,
		Example: `./examples tables --dataset 1k_trips`,
		Run: func(cmd *cobra.Command, args []string) {
			createTablesForDataSet()
		},
	}

	queryCmd := &cobra.Command{
		Use: "query",
		Short:    `Run sample queries against example dataset`,
		Long:    `Run sample queries against example dataset`,
		Example: `./examples query --dataset 1k_trips`,
		Run: func(cmd *cobra.Command, args []string) {
			queryDataSet()
		},
	}

	rootCmd.AddCommand(tableCmd, dataCmd, queryCmd)
	rootCmd.Execute()
}

package main

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uber/aresdb/examples/utils"
)

const (
	schemaDir  = "schema"
	queriesDir = "queries"
	dataDir    = "data"

	dataSetKeyName = "dataset"
	hostKeyName    = "host"
	portKeyName    = "port"
)

func queryDataSet() {
	dataSetName := viper.GetString(dataSetKeyName)
	dataSetQueriesDir := fmt.Sprintf("%s/%s", dataSetName, queriesDir)
	queriesDirInfo, err := ioutil.ReadDir(dataSetQueriesDir)
	utils.PanicIfErr(err)
	host := viper.GetString(hostKeyName)
	port := viper.GetInt(portKeyName)

	for _, queryInfo := range queriesDirInfo {
		baseName := queryInfo.Name()
		queryName := strings.TrimSuffix(baseName, filepath.Ext(baseName))
		queryType := filepath.Ext(baseName)
		queryPath := fmt.Sprintf("%s/%s/%s", dataSetName, queriesDir, baseName)
		utils.MakeQuery(host, port, queryName, queryType, queryPath)
	}
}

func createTablesForDataSet() {
	dataSetName := viper.GetString(dataSetKeyName)
	dataSetSchemaDir := fmt.Sprintf("%s/%s", dataSetName, schemaDir)
	schemaDirInfo, err := ioutil.ReadDir(dataSetSchemaDir)
	utils.PanicIfErr(err)
	host := viper.GetString(hostKeyName)
	port := viper.GetInt(portKeyName)

	for _, schemaInfo := range schemaDirInfo {
		baseName := schemaInfo.Name()
		tableName := strings.TrimSuffix(baseName, filepath.Ext(baseName))
		tableSchemaPath := fmt.Sprintf("%s/%s/%s", dataSetName, schemaDir, baseName)
		utils.CreateTable(host, port, tableName, tableSchemaPath)
	}
}

func ingestDataForDataSet() {
	dataSetName := viper.GetString(dataSetKeyName)
	dataFileDir := fmt.Sprintf("./%s/%s", dataSetName, dataDir)
	dataFiles, err := ioutil.ReadDir(dataFileDir)
	utils.PanicIfErr(err)
	host := viper.GetString(hostKeyName)
	port := viper.GetInt(portKeyName)

	for _, dataFileInfo := range dataFiles {
		baseName := dataFileInfo.Name()
		dataFilePath := fmt.Sprintf("./%s/%s/%s", dataSetName, dataDir, baseName)
		tableName := strings.TrimSuffix(baseName, filepath.Ext(baseName))
		utils.IngestDataForTable(host, port, tableName, dataFilePath)
	}
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
		Use:     "data",
		Short:   "Ingest data for example dataset",
		Long:    `Ingest data for example dataset`,
		Example: `./examples data --dataset 1k_trips`,
		Run: func(cmd *cobra.Command, args []string) {
			ingestDataForDataSet()
		},
	}

	tableCmd := &cobra.Command{
		Use:     "tables",
		Short:   `Create tables for example dataset`,
		Long:    `Create tables for example dataset`,
		Example: `./examples tables --dataset 1k_trips`,
		Run: func(cmd *cobra.Command, args []string) {
			createTablesForDataSet()
		},
	}

	queryCmd := &cobra.Command{
		Use:     "query",
		Short:   `Run sample queries against example dataset`,
		Long:    `Run sample queries against example dataset`,
		Example: `./examples query --dataset 1k_trips`,
		Run: func(cmd *cobra.Command, args []string) {
			queryDataSet()
		},
	}

	rootCmd.AddCommand(tableCmd, dataCmd, queryCmd)
	rootCmd.Execute()
}

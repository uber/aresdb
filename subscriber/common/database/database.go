package database

import (
	memCom "github.com/uber/aresdb/memstore/common"
)

// Row represents a single database row
type Row []interface{}

// Database is abstraction for interactions with downstream storage layer
type Database interface {
	// Cluster returns the DB cluster name
	Cluster() string

	// Save will save the rows into underlying database
	Save(destination Destination, rows []Row) error

	// Shutdown will close the connections to the database
	Shutdown()
}

// Destination contains the table and columns that each job is storing data into
// also records the behavior when encountering key errors
type Destination struct {
	Table           string
	ColumnNames     []string
	PrimaryKeys     map[string]interface{}
	AresUpdateModes []memCom.ColumnUpdateMode
}

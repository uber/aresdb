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

package database

import (
	"github.com/uber/aresdb/client"
	memCom "github.com/uber/aresdb/memstore/common"
)

// Database is abstraction for interactions with downstream storage layer
type Database interface {
	// Cluster returns the DB cluster name
	Cluster() string

	// Save will save the rows into underlying database
	Save(destination Destination, rows []client.Row) error

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

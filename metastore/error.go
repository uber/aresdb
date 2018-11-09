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

package metastore

import (
	"errors"
)

var (
	// ErrTableDoesNotExist indicates Table does not exist
	ErrTableDoesNotExist = errors.New("Table does not exist")
	// ErrTableAlreadyExist indicates Table already exists
	ErrTableAlreadyExist = errors.New("Table already exists")
	// ErrColumnDoesNotExist indicates Column does not exist error
	ErrColumnDoesNotExist = errors.New("Column does not exist")
	// ErrColumnAlreadyExist indicates Column already exists
	ErrColumnAlreadyExist = errors.New("Column already exists")
	// ErrColumnAlreadyDeleted indicates Column already deleted
	ErrColumnAlreadyDeleted = errors.New("Column already deleted")
	// ErrNotEnumColumn indicates Column is not enum type
	ErrNotEnumColumn = errors.New("Column is not enum type")
	// ErrShardDoesNotExist indicates Shard does not exist
	ErrShardDoesNotExist = errors.New("Shard does not exist")
	// ErrNotFactTable indicates table not a fact table
	ErrNotFactTable = errors.New("Table is not fact table")
	// ErrNotDimensionTable indicates table is not a dimension table
	ErrNotDimensionTable = errors.New("Table is not dimension table")
	// ErrWatcherAlreadyExist indicates table is not a dimension table
	ErrWatcherAlreadyExist = errors.New("Watcher already registered")
	// ErrDeleteTimeColumn indicates column is time column and cannot be deleted
	ErrDeleteTimeColumn = errors.New("Time column cannot be deleted")
	// ErrDeletePrimaryKeyColumn indicates column belongs to primary key cannot be deleted
	ErrDeletePrimaryKeyColumn = errors.New("Primary key column cannot be deleted")
	// ErrInvalidTableSchema indicates a table schema is not valid
	ErrInvalidTableSchema = errors.New("Table schema is not valid")
	// ErrInvalidSchemaUpdate indicates it's impossible to apply an update to a table schema
	ErrInvalidSchemaUpdate = errors.New("Table schema is to update not valid")
)

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
	// ErrChangePrimaryKeyColumn indicates primary key columns cannot be changed
	ErrChangePrimaryKeyColumn = errors.New("Primary key column cannot be changed")
	// ErrAllColumnsInvalid indicates all columns are invalid
	ErrAllColumnsInvalid = errors.New("All columns are invalid")
	// ErrMissingPrimaryKey indicates a schema does not have primary key
	ErrMissingPrimaryKey = errors.New("Primary key columns not specified")
	// ErrColumnNonExist indicates a column used does not exist
	ErrColumnNonExist= errors.New("Column does not exist")
	// ErrColumnDeleted indicates a column used was deleted
	ErrColumnDeleted= errors.New("Column already deleted")
	// ErrInvalidDataType indicates invalid data type
	ErrInvalidDataType = errors.New("Invalid data type")
	// ErrIllegalSchemaVersion indicates new schema is not greater than old one
	ErrIllegalSchemaVersion = errors.New("New schema version not greater than old")
	// ErrSchemaUpdateNotAllowed indicates changes attemped on immutable fields
	ErrSchemaUpdateNotAllowed = errors.New("Illegal schame update on immutable field")
	// ErrInsufficientColumnCount indicates no column in a schame
	ErrInsufficientColumnCount = errors.New("Insufficient column count")
	// ErrReusingColumnIDNotAllowed indicates attempt to reuse id of deleted column
	ErrReusingColumnIDNotAllowed = errors.New("Reusing column id not allowed")
	// ErrNewColumnWithDeletion indicates adding a new column with deleted flag on
	ErrNewColumnWithDeletion = errors.New("Can not add column with deleted flag on")
	// ErrIllegalChangeSortColumn indicates illegal changes on sort columns
	ErrIllegalChangeSortColumn = errors.New("Illegal changes on sort columns")
)

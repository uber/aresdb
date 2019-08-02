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

package common

import (
	"fmt"
	"github.com/uber/aresdb/memstore/vectors"
	"os"
	"sync"
)

// BatchReader defines the interface to retrieve a DataValue given a row index and
// column index.
type BatchReader interface {
	GetDataValue(row, columnID int) DataValue
	GetDataValueWithDefault(row, columnID int, defaultValue DataValue) DataValue
}

// Batch represents a sorted or live batch.
type Batch struct {
	// Batch mutex is locked in reader mode by queries during the entire transfer
	// to ensure row level consistent read. It is locked in writer mode only for
	// updates from ingestion, and for modifications to the columns slice itself
	// (e.g., adding new columns). Appends will update LastReadBatchID and
	// NumRecordsInLastWriteBatch to make newly added records visible only at the last
	// step, therefore the batch does not need to be locked for appends.
	// For sorted bathes this is also locked in writer mode for initiating loading
	// from disk (vector party creation, Loader/Users initialization), and for
	// vector party detaching during eviction.
	*sync.RWMutex
	// For live batches, index out of bound and nil VectorParty indicates
	// mode 0 for the corresponding VectorParty.
	// For archive batches, index out of bound and nil VectorParty indicates that
	// the corresponding VectorParty has not been loaded into memory from disk.
	Columns []vectors.VectorParty
}

// GetVectorParty returns the VectorParty for the specified column from
// the batch. It requires the batch to be locked for reading.
func (b *Batch) GetVectorParty(columnID int) vectors.VectorParty {
	if columnID >= len(b.Columns) {
		return nil
	}
	return b.Columns[columnID]
}

// GetDataValue read value from underlying columns.
func (b *Batch) GetDataValue(row, columnID int) DataValue {
	if columnID >= len(b.Columns) {
		return NullDataValue
	}
	vp := b.Columns[columnID]
	if vp == nil {
		return NullDataValue
	}
	return vp.GetDataValue(row)
}

// GetDataValueWithDefault read value from underlying columns and if it's missing, it will return
// passed value instead.
func (b *Batch) GetDataValueWithDefault(row, columnID int, defaultValue DataValue) DataValue {
	if columnID >= len(b.Columns) {
		return defaultValue
	}
	vp := b.Columns[columnID]
	if vp == nil {
		return defaultValue
	}
	return vp.GetDataValue(row)
}

// SafeDestruct destructs all vector parties of this batch.
func (b *Batch) SafeDestruct() {
	if b != nil {
		for _, col := range b.Columns {
			if col != nil {
				col.SafeDestruct()
			}
		}
	}
}

// Equals check whether two batches are the same. Notes both batches should have all its columns loaded into memory
// before comparison. Therefore this function should be only called for unit test purpose.
func (b *Batch) Equals(other *Batch) bool {
	if b == nil || other == nil {
		return b == nil && other == nil
	}
	if len(b.Columns) != len(other.Columns) {
		return false
	}
	for columnID := range b.Columns {
		if !vectors.VectorPartyEquals(b.Columns[columnID], other.Columns[columnID]) {
			return false
		}
	}
	return true
}

func (b *Batch) Dump(file *os.File) {
	fmt.Fprintf(file, "Dump Batch, columns: %d\n", len(b.Columns))
	for i, col := range b.Columns {
		fmt.Fprintf(file, "col: %d\n", i)
		if col != nil {
			col.Dump(file)
		}
	}
}

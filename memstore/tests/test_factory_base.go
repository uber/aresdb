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

// Package memstore has to put test factory here since otherwise we will have a
// memstore -> utils -> memstore import cycle.

package tests

import (
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memstore/vectors"
	"github.com/uber/aresdb/utils"
	"gopkg.in/yaml.v2"
	"path/filepath"
	"strings"

	"sync"
)

// TestFactoryT creates memstore test objects from text file
type TestFactoryBase struct {
	RootPath string
	utils.FileSystem
	// functions to do real vp conversion, need to pass in from caller
	ToArchiveVectorParty func(common.VectorParty, sync.Locker) common.ArchiveVectorParty
	ToLiveVectorParty    func(common.VectorParty) common.LiveVectorParty
	ToVectorParty        func(*RawVectorParty, bool) (common.VectorParty, error)
}

type rawBatch struct {
	Columns []string `yaml:"columns"`
}

type RawVectorParty struct {
	DataType  string   `yaml:"data_type"`
	Length    int      `yaml:"length"`
	Values    []string `yaml:"values"`
	HasCounts bool     `yaml:"has_counts"`
}

type rawVector struct {
	DataType string   `yaml:"data_type"`
	Length   int      `yaml:"length"`
	Values   []string `yaml:"values"`
}

// rawUpsertBatch represents the upsert batch format in a yaml file. Each individual upsert batch consists of two parts
// columns and rows. Each column needs to specify the column type and column id. Column type need to be a valid type
// defined in common.data_type.go. Each row consists of column values which are comma splitted.
type rawUpsertBatch struct {
	Columns []struct {
		ColumnID int    `yaml:"column_id"`
		DataType string `yaml:"data_type"`
	} `yaml:"columns"`
	Rows []string `yaml:"rows"`
}

// ReadArchiveBatch read batch and do pruning for every columns.
func (t TestFactoryBase) ReadArchiveBatch(name string) (*common.Batch, error) {
	batch, err := t.ReadBatch(name, false)
	if err != nil {
		return nil, err
	}
	for i, column := range batch.Columns {
		if column != nil {
			archiveColumn := t.ToArchiveVectorParty(column, batch)
			batch.Columns[i] = archiveColumn

		}
	}
	batch.RWMutex = &sync.RWMutex{}
	return batch, nil
}

// ReadLiveBatch read batch and skip pruning for every columns.
func (t TestFactoryBase) ReadLiveBatch(name string) (*common.Batch, error) {
	batch, err := t.ReadBatch(name, true)
	if err != nil {
		return nil, err
	}
	for i, column := range batch.Columns {
		if column != nil {
			liveColumn := t.ToLiveVectorParty(column)
			batch.Columns[i] = liveColumn
		}
	}
	batch.RWMutex = &sync.RWMutex{}
	return batch, nil
}

// ReadBatch returns a batch given batch name. Batch will be searched
// under testing/data/batches folder. Prune tells whether need to prune
// the columns after column contruction.
func (t TestFactoryBase) ReadBatch(name string, forLiveVP bool) (*common.Batch, error) {
	path := filepath.Join(t.RootPath, "batches", name)
	return t.readBatchFromFile(path, forLiveVP)
}

func (t TestFactoryBase) readBatchFromFile(path string, forLiveVP bool) (*common.Batch, error) {
	fileContent, err := t.ReadFile(path)
	if err != nil {
		return nil, err
	}

	rb := &rawBatch{}
	if err = yaml.Unmarshal(fileContent, &rb); err != nil {
		return nil, err
	}
	return rb.toBatch(t, forLiveVP)
}

func (rb *rawBatch) toBatch(t TestFactoryBase, forLiveVP bool) (*common.Batch, error) {
	batch := &common.Batch{
		Columns: make([]common.VectorParty, len(rb.Columns)),
	}
	for i, name := range rb.Columns {
		if len(name) == 0 {
			continue
		}
		column, err := t.ReadVectorParty(name, forLiveVP)
		if err != nil {
			return nil, utils.StackError(err,
				"Failed to read vector party %s",
				name,
			)
		}
		batch.Columns[i] = column
	}
	return batch, nil
}

// ReadArchiveVectorParty loads a vector party and prune it after construction.
func (t TestFactoryBase) ReadArchiveVectorParty(name string, locker sync.Locker) (common.ArchiveVectorParty, error) {
	vp, err := t.ReadVectorParty(name, false)
	if err != nil {
		return nil, err
	}
	return t.ToArchiveVectorParty(vp, locker), nil
}

// ReadLiveVectorParty loads a vector party and skip pruning.
func (t TestFactoryBase) ReadLiveVectorParty(name string) (common.LiveVectorParty, error) {
	vp, err := t.ReadVectorParty(name, true)
	if err != nil {
		return nil, err
	}
	return t.ToLiveVectorParty(vp), nil
}

// ReadVectorParty returns a vector party given vector party name. Vector party
// will be searched under testing/data/vps folder. Prune tells whether to prune this
// column.
func (t TestFactoryBase) ReadVectorParty(name string, forLiveVP bool) (common.VectorParty, error) {
	path := filepath.Join(t.RootPath, "vps", name)
	return t.readVectorPartyFromFile(path, forLiveVP)
}

func (t TestFactoryBase) readVectorPartyFromFile(path string, forLiveVP bool) (common.VectorParty, error) {
	fileContent, err := t.ReadFile(path)
	if err != nil {
		return nil, err
	}

	rvp := &RawVectorParty{}
	if err = yaml.Unmarshal(fileContent, &rvp); err != nil {
		return nil, err
	}
	return t.ToVectorParty(rvp, forLiveVP)
}

// ReadVector returns a vector given vector name. Vector will
// be searched under testing/data/vectors folder.
func (t TestFactoryBase) ReadVector(name string) (*vectors.Vector, error) {
	path := filepath.Join(t.RootPath, "vectors", name)
	return t.readVectorFromFile(path)
}

func setDataValue(v *vectors.Vector, idx int, val common.DataValue) {
	if val.Valid {
		if v.DataType == common.Bool {
			v.SetBool(idx, val.BoolVal)
		} else {
			v.SetValue(idx, val.OtherVal)
		}
	}
}

func (rv *rawVector) toVector() (*vectors.Vector, error) {
	dataType := common.DataTypeFromString(rv.DataType)
	if dataType == common.Unknown {
		return nil, utils.StackError(nil,
			"Unknown DataType when reading vector from file",
		)
	}

	if len(rv.Values) != rv.Length {
		return nil, utils.StackError(nil,
			"Values length %d is not as expected: %d",
			len(rv.Values),
			rv.Length,
		)
	}

	v := vectors.NewVector(dataType, rv.Length)

	for i, row := range rv.Values {
		val, err := common.ValueFromString(row, dataType)
		if err != nil {
			return nil, utils.StackError(err,
				"Unable to parse value from string %s for data type %s",
				row, rv.DataType)
		}
		setDataValue(v, i, val)
	}
	return v, nil
}

func (t TestFactoryBase) readVectorFromFile(path string) (*vectors.Vector, error) {
	fileContent, err := t.ReadFile(path)
	if err != nil {
		return nil, err
	}

	rv := &rawVector{}
	if err = yaml.Unmarshal(fileContent, &rv); err != nil {
		return nil, err
	}

	return rv.toVector()
}

// ReadUpsertBatch returns a pointer to UpsertBatch given the upsert batch name.
func (t TestFactoryBase) ReadUpsertBatch(name string) (*common.UpsertBatch, error) {
	path := filepath.Join(t.RootPath, "upsert-batches", name)
	return t.readUpsertBatchFromFile(path)
}

func (ru *rawUpsertBatch) toUpsertBatch() (*common.UpsertBatch, error) {
	builder := common.NewUpsertBatchBuilder()
	var dataTypes []common.DataType
	for _, column := range ru.Columns {
		dataType := common.DataTypeFromString(column.DataType)
		if dataType == common.Unknown {
			return nil, utils.StackError(nil,
				"Unknown DataType when reading vector from file",
			)
		}
		dataTypes = append(dataTypes, dataType)
		if err := builder.AddColumn(column.ColumnID, dataType); err != nil {
			return nil, err
		}
	}

	for row, rowStr := range ru.Rows {
		builder.AddRow()
		rawValues := strings.Split(rowStr, ";")
		if len(rawValues) != len(ru.Columns) {
			return nil, utils.StackError(nil,
				"Length of rawValues %d on row %d is different from number of columns %d", len(rawValues), row, len(ru.Columns))
		}

		for col, rawValue := range rawValues {
			value, err := common.ValueFromString(rawValue, dataTypes[col])
			if err != nil {
				return nil, err
			}
			builder.SetValue(row, col, value.ConvertToHumanReadable(dataTypes[col]))
		}
	}

	bytes, err := builder.ToByteArray()
	if err != nil {
		return nil, err
	}
	return common.NewUpsertBatch(bytes)
}

func (t TestFactoryBase) readUpsertBatchFromFile(path string) (*common.UpsertBatch, error) {
	fileContent, err := t.ReadFile(path)
	if err != nil {
		return nil, err
	}

	ru := &rawUpsertBatch{}
	if err = yaml.Unmarshal(fileContent, &ru); err != nil {
		return nil, err
	}

	return ru.toUpsertBatch()
}

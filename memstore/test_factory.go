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
package memstore

import (
	"path/filepath"

	"github.com/uber/aresdb/utils"

	"github.com/uber/aresdb/common"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/redolog"
	"gopkg.in/yaml.v2"
	"strings"
	"sync"
	"github.com/stretchr/testify/mock"
)

const (
	vpValueTestDelimiter = ","
)

var (
	testFactory = TestFactoryT{
		RootPath:   "../testing/data",
		FileSystem: utils.OSFileSystem{},
	}
)

// TestFactoryT creates memstore test objects from text file
type TestFactoryT struct {
	RootPath string
	utils.FileSystem
}

// NewMockMemStore returns a new memstore with mocked diskstore and metastore.
func (t TestFactoryT) NewMockMemStore() *memStoreImpl {
	metaStore := new(metaMocks.MetaStore)
	diskStore := new(diskMocks.DiskStore)
	redoLogManagerMaster, _ := redolog.NewRedoLogManagerMaster(&common.RedoLogConfig{}, diskStore, metaStore)
	bootstrapToken := new(memComMocks.BootStrapToken)
	bootstrapToken.On("AcquireToken",  mock.Anything, mock.Anything).Return(true)
	bootstrapToken.On("ReleaseToken",  mock.Anything, mock.Anything).Return()

	return NewMemStore(metaStore, diskStore, NewOptions(bootstrapToken, redoLogManagerMaster)).(*memStoreImpl)
}

// ReadArchiveBatch read batch and do pruning for every columns.
func (t TestFactoryT) ReadArchiveBatch(name string) (*Batch, error) {
	batch, err := t.ReadBatch(name)
	if err != nil {
		return nil, err
	}
	for i, column := range batch.Columns {
		if column != nil {
			archiveColumn := convertToArchiveVectorParty(column.(*cVectorParty), batch)
			batch.Columns[i] = archiveColumn
		}
	}
	batch.RWMutex = &sync.RWMutex{}
	return batch, nil
}

func convertCLiveVectorParty(vp *cVectorParty) *cLiveVectorParty {
	return &cLiveVectorParty{
		cVectorParty: *vp,
	}
}

func convertToArchiveVectorParty(vp *cVectorParty, locker sync.Locker) *archiveVectorParty {
	archiveColumn := &archiveVectorParty{
		cVectorParty: *vp,
	}
	archiveColumn.allUsersDone = sync.NewCond(locker)
	archiveColumn.Prune()
	return archiveColumn
}

// ReadLiveBatch read batch and skip pruning for every columns.
func (t TestFactoryT) ReadLiveBatch(name string) (*Batch, error) {
	batch, err := t.ReadBatch(name)
	if err != nil {
		return nil, err
	}
	for i, column := range batch.Columns {
		if column != nil {
			liveColumn := convertCLiveVectorParty(column.(*cVectorParty))
			batch.Columns[i] = liveColumn
		}
	}
	batch.RWMutex = &sync.RWMutex{}
	return batch, nil
}

// ReadBatch returns a batch given batch name. Batch will be searched
// under testing/data/batches folder. Prune tells whether need to prune
// the columns after column contruction.
func (t TestFactoryT) ReadBatch(name string) (*Batch, error) {
	path := filepath.Join(t.RootPath, "batches", name)
	return t.readBatchFromFile(path)
}

type rawBatch struct {
	Columns []string `yaml:"columns"`
}

func (t TestFactoryT) readBatchFromFile(path string) (*Batch, error) {
	fileContent, err := t.ReadFile(path)
	if err != nil {
		return nil, err
	}

	rb := &rawBatch{}
	if err = yaml.Unmarshal(fileContent, &rb); err != nil {
		return nil, err
	}
	return rb.toBatch(t)
}

func (rb *rawBatch) toBatch(t TestFactoryT) (*Batch, error) {
	batch := &Batch{
		Columns: make([]memCom.VectorParty, len(rb.Columns)),
	}
	for i, name := range rb.Columns {
		if len(name) == 0 {
			continue
		}
		column, err := t.ReadVectorParty(name)
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
func (t TestFactoryT) ReadArchiveVectorParty(name string, locker sync.Locker) (*archiveVectorParty, error) {
	vp, err := t.ReadVectorParty(name)
	if err != nil {
		return nil, err
	}
	return convertToArchiveVectorParty(vp, locker), nil
}

// ReadLiveVectorParty loads a vector party and skip pruning.
func (t TestFactoryT) ReadLiveVectorParty(name string) (*cLiveVectorParty, error) {
	vp, err := t.ReadVectorParty(name)
	if err != nil {
		return nil, err
	}
	return convertCLiveVectorParty(vp), nil
}

// ReadVectorParty returns a vector party given vector party name. Vector party
// will be searched under testing/data/vps folder. Prune tells whether to prune this
// column.
func (t TestFactoryT) ReadVectorParty(name string) (*cVectorParty, error) {
	path := filepath.Join(t.RootPath, "vps", name)
	return t.readVectorPartyFromFile(path)
}

type rawVectorParty struct {
	DataType  string   `yaml:"data_type"`
	HasCounts bool     `yaml:"has_counts"`
	Length    int      `yaml:"length"`
	Values    []string `yaml:"values"`
}

func (t TestFactoryT) readVectorPartyFromFile(path string) (*cVectorParty, error) {
	fileContent, err := t.ReadFile(path)
	if err != nil {
		return nil, err
	}

	rvp := &rawVectorParty{}
	if err = yaml.Unmarshal(fileContent, &rvp); err != nil {
		return nil, err
	}
	return rvp.toVectorParty()
}

func (rvp *rawVectorParty) toVectorParty() (*cVectorParty, error) {
	dataType := memCom.DataTypeFromString(rvp.DataType)
	if dataType == memCom.Unknown {
		return nil, utils.StackError(nil,
			"Unknown DataType when reading vector from file",
		)
	}

	if len(rvp.Values) != 0 && len(rvp.Values) != rvp.Length {
		return nil, utils.StackError(nil,
			"Values length %d is not as expected and it's not a mode 0 vp: %d",
			len(rvp.Values),
			rvp.Length,
		)
	}

	var countsVec *Vector
	columnMode := memCom.HasNullVector
	if rvp.HasCounts {
		countsVec = NewVector(
			memCom.Uint32,
			rvp.Length+1,
		)
		columnMode = memCom.HasCountVector
	}

	vp := &cVectorParty{
		baseVectorParty: baseVectorParty{
			length:   rvp.Length,
			dataType: dataType,
		},
		columnMode: columnMode,
		values: NewVector(
			dataType,
			rvp.Length,
		),
		nulls: NewVector(
			dataType,
			rvp.Length,
		),
		counts: countsVec,
	}

	var prevCount uint32
	for i, row := range rvp.Values {
		values := strings.SplitN(row, vpValueTestDelimiter, 2)
		if len(values) < 1 {
			return nil, utils.StackError(nil,
				"Each row should have at least one value",
			)
		}

		val, err := memCom.ValueFromString(values[0], dataType)
		if err != nil {
			return nil, utils.StackError(err,
				"Unable to parse value from string %s for data type %d",
				values[0], dataType)
		}

		if rvp.HasCounts {
			if len(values) < 2 {
				return nil, utils.StackError(err,
					"Each row should have two values if "+
						"has_count is specified to be true",
				)
			}
			currentCountVal, err := memCom.ValueFromString(values[1], memCom.Uint32)

			if currentCountVal.Valid == false {
				return nil, utils.StackError(err,
					"Count value cannot be be null")
			}
			if err != nil {
				return nil, utils.StackError(err,
					"Unable to parse value from string %s for data type %d",
					values[1], memCom.Uint32)
			}
			setDataValue(countsVec, i+1, currentCountVal)
			currentCount := *(*uint32)(currentCountVal.OtherVal)
			vp.SetDataValue(i, val, IncrementCount, currentCount-prevCount)
			prevCount = currentCount
		} else {
			vp.SetDataValue(i, val, IncrementCount)
		}
	}

	return vp, nil
}

// ReadVector returns a vector given vector name. Vector will
// be searched under testing/data/vectors folder.
func (t TestFactoryT) ReadVector(name string) (*Vector, error) {
	path := filepath.Join(t.RootPath, "vectors", name)
	return t.readVectorFromFile(path)
}

type rawVector struct {
	DataType string   `yaml:"data_type"`
	Length   int      `yaml:"length"`
	Values   []string `yaml:"values"`
}

func setDataValue(v *Vector, idx int, val memCom.DataValue) {
	if val.Valid {
		if v.DataType == memCom.Bool {
			v.SetBool(idx, val.BoolVal)
		} else {
			v.SetValue(idx, val.OtherVal)
		}
	}
}

func (rv *rawVector) toVector() (*Vector, error) {
	dataType := memCom.DataTypeFromString(rv.DataType)
	if dataType == memCom.Unknown {
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

	v := NewVector(dataType, rv.Length)

	for i, row := range rv.Values {
		val, err := memCom.ValueFromString(row, dataType)
		if err != nil {
			return nil, utils.StackError(err,
				"Unable to parse value from string %s for data type %s",
				row, rv.DataType)
		}
		setDataValue(v, i, val)
	}
	return v, nil
}

func (t TestFactoryT) readVectorFromFile(path string) (*Vector, error) {
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
func (t TestFactoryT) ReadUpsertBatch(name string) (*memCom.UpsertBatch, error) {
	path := filepath.Join(t.RootPath, "upsert-batches", name)
	return t.readUpsertBatchFromFile(path)
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

func (ru *rawUpsertBatch) toUpsertBatch() (*memCom.UpsertBatch, error) {
	builder := memCom.NewUpsertBatchBuilder()
	var dataTypes []memCom.DataType
	for _, column := range ru.Columns {
		dataType := memCom.DataTypeFromString(column.DataType)
		if dataType == memCom.Unknown {
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
		rawValues := strings.Split(rowStr, ",")
		if len(rawValues) != len(ru.Columns) {
			return nil, utils.StackError(nil,
				"Length of rawValues %d on row %d is different from number of columns %d", len(rawValues), row, len(ru.Columns))
		}

		for col, rawValue := range rawValues {
			value, err := memCom.ValueFromString(rawValue, dataTypes[col])
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
	return memCom.NewUpsertBatch(bytes)
}

func (t TestFactoryT) readUpsertBatchFromFile(path string) (*memCom.UpsertBatch, error) {
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

func getFactory() TestFactoryT {
	return testFactory
}

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
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/common"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	"github.com/uber/aresdb/memstore/list"
	"github.com/uber/aresdb/memstore/tests"
	"github.com/uber/aresdb/memstore/vectors"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/redolog"
	"github.com/uber/aresdb/utils"
	"strings"
	"sync"
)

const (
	vpValueTestDelimiter = ","
)

var (
	testFactory = TestFactoryT{
		TestFactoryBase: tests.TestFactoryBase{
			RootPath:             "../testing/data",
			FileSystem:           utils.OSFileSystem{},
			ToArchiveVectorParty: toArchiveVectorParty,
			ToLiveVectorParty:    toLiveVectorParty,
			ToVectorParty:        toVectorParty,
		},
	}
)

// TestFactoryT creates memstore test objects from text file
type TestFactoryT struct {
	tests.TestFactoryBase
}

// NewMockMemStore returns a new memstore with mocked diskstore and metastore.
func (t TestFactoryT) NewMockMemStore() *memStoreImpl {
	metaStore := new(metaMocks.MetaStore)
	diskStore := new(diskMocks.DiskStore)
	redoLogManagerMaster, _ := redolog.NewRedoLogManagerMaster("", &common.RedoLogConfig{}, diskStore, metaStore)
	bootstrapToken := new(memComMocks.BootStrapToken)
	bootstrapToken.On("AcquireToken", mock.Anything, mock.Anything).Return(true)
	bootstrapToken.On("ReleaseToken", mock.Anything, mock.Anything).Return()

	return NewMemStore(metaStore, diskStore, NewOptions(bootstrapToken, redoLogManagerMaster)).(*memStoreImpl)
}

func toArchiveVectorParty(vp vectors.VectorParty, locker sync.Locker) vectors.ArchiveVectorParty {
	if vp.IsList() {
		return list.ToArrayArchiveVectorParty(vp, locker)
	}
	archiveColumn := &archiveVectorParty{
		cVectorParty: *vp.(*cVectorParty),
	}
	archiveColumn.AllUsersDone = sync.NewCond(locker)
	archiveColumn.Prune()
	return archiveColumn
}

func toLiveVectorParty(vp vectors.VectorParty) vectors.LiveVectorParty {
	if vp.IsList() {
		return list.ToArrayLiveVectorParty(vp)
	}
	return &cLiveVectorParty{
		cVectorParty: *vp.(*cVectorParty),
	}
}

func toVectorParty(rvp *RawVectorParty, forLiveVP bool) (vectors.VectorParty, error) {
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

	if memCom.IsArrayType(dataType) {
		return list.ToArrayVectorParty(rvp, forLiveVP)
	}

	var countsVec *vectors.Vector
	columnMode := vectors.HasNullVector
	if rvp.HasCounts {
		countsVec = vectors.NewVector(
			memCom.Uint32,
			rvp.Length+1,
		)
		columnMode = vectors.HasCountVector
	}

	vp := &cVectorParty{
		baseVectorParty: baseVectorParty{
			length:   rvp.Length,
			dataType: dataType,
		},
		columnMode: columnMode,
		values: vectors.NewVector(
			dataType,
			rvp.Length,
		),
		nulls: vectors.NewVector(
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
			vp.SetDataValue(i, val, vectors.IncrementCount, currentCount-prevCount)
			prevCount = currentCount
		} else {
			vp.SetDataValue(i, val, vectors.IncrementCount)
		}
	}

	return vp, nil
}

func setDataValue(v *vectors.Vector, idx int, val memCom.DataValue) {
	if val.Valid {
		if v.DataType == memCom.Bool {
			v.SetBool(idx, val.BoolVal)
		} else {
			v.SetValue(idx, val.OtherVal)
		}
	}
}

func GetFactory() TestFactoryT {
	return testFactory
}

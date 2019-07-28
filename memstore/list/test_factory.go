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

package list

import (
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"sync"
)

var (
	testFactory = TestFactoryT{
		TestFactoryT: memCom.TestFactoryT{
			RootPath:             "../../testing/data",
			FileSystem:           utils.OSFileSystem{},
			ToArchiveVectorParty: ToArrayArchiveVectorParty,
			ToLiveVectorParty:    ToArrayLiveVectorParty,
			ToVectorParty:        ToArrayVectorParty,
		},
	}
)

// TestFactoryT creates test objects from text file
type TestFactoryT struct {
	memCom.TestFactoryT
}

func GetFactory() TestFactoryT {
	return testFactory
}

func ToArrayArchiveVectorParty(vp memCom.VectorParty, locker sync.Locker) memCom.ArchiveVectorParty {
	return vp.(memCom.ArchiveVectorParty)
}

func ToArrayLiveVectorParty(vp memCom.VectorParty) memCom.LiveVectorParty {
	return vp.(memCom.LiveVectorParty)
}

func ToArrayVectorParty(rvp *memCom.RawVectorParty, forLiveVP bool) (vp memCom.VectorParty, err error) {
	dataType := memCom.DataTypeFromString(rvp.DataType)
	if dataType == memCom.Unknown {
		return nil, utils.StackError(nil,
			"Unknown DataType when reading vector from file",
		)
	}

	if len(rvp.Values) != 0 && len(rvp.Values) != rvp.Length {
		return nil, utils.StackError(nil,
			"List values length %d is not as expected %d",
			len(rvp.Values),
			rvp.Length,
		)
	}

	// array live party
	if forLiveVP {
		vp = NewLiveVectorParty(rvp.Length, dataType, nil)
		vp.Allocate(false)
		for i, row := range rvp.Values {
			val, err := memCom.ValueFromString(row, dataType)
			if err != nil {
				return nil, err
			}
			vp.SetDataValue(i, val, memCom.IgnoreCount)
		}
		return vp, nil
	}

	// array archive party
	var totalBytes int64
	values := make([]memCom.DataValue, rvp.Length)
	for i, row := range rvp.Values {
		if values[i], err = memCom.ValueFromString(row, dataType); err != nil {
			return nil, err
		}
		if values[i].Valid {
			reader := memCom.NewArrayValueReader(dataType, values[i].OtherVal)
			totalBytes += int64(memCom.CalculateListElementBytes(dataType, reader.GetLength()))
		}
	}

	vp = NewArchiveVectorParty(rvp.Length, dataType, totalBytes, &sync.RWMutex{})
	vp.Allocate(false)
	for i, val := range values {
		vp.SetDataValue(i, val, memCom.IgnoreCount)
	}
	return
}

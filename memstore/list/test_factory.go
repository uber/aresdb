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
package list

import (
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"gopkg.in/yaml.v2"
	"path/filepath"
	"unsafe"
)

const (
	vpValueTestDelimiter = ','
)

var (
	testFactory = TestFactoryT{
		TestFactoryT: memstore.TestFactoryT{
			RootPath:   "../../testing/data",
			FileSystem: utils.OSFileSystem{},
		},
	}

	// To ignore empty token which strings.Split cannot do.
	vpValueSplitFunc = func(c rune) bool {
		return c == vpValueTestDelimiter
	}
)

// TestFactoryT creates list vp test objects from text file.
type TestFactoryT struct {
	memstore.TestFactoryT
}

// ReadListVectorParty loads a list vector party. To be simple, it will
func (t TestFactoryT) ReadListVectorParty(name string) (memCom.VectorParty, error) {
	path := filepath.Join(t.RootPath, "vps/list", name)
	return t.readListVectorPartyFromFile(path)
}

type rawListVectorParty struct {
	DataType string   `yaml:"data_type"`
	Length   int      `yaml:"length"`
	Values   []string `yaml:"values"`
}

func (t TestFactoryT) readListVectorPartyFromFile(path string) (memCom.VectorParty, error) {
	fileContent, err := t.ReadFile(path)
	if err != nil {
		return nil, err
	}

	rvp := &rawListVectorParty{}
	if err = yaml.Unmarshal(fileContent, &rvp); err != nil {
		return nil, err
	}
	return rvp.toVectorParty()
}

// testListDataValueReader implements the ListDataValueReader interface via pre-fetching everything into a 2d array of
// DataValues
type testListDataValueReader struct {
	values [][]memCom.DataValue
	length int
}

// ReadElementValue implements ReadElementValue in ListDataValueReader.
func (r testListDataValueReader) ReadElementValue(row, column int) unsafe.Pointer {
	return r.values[row][column].OtherVal
}

// ReadElementBool implements ReadElementBool in ListDataValueReader.
func (r testListDataValueReader) ReadElementBool(row, column int) bool {
	return r.values[row][column].BoolVal
}

// ReadElementValidity implements ReadElementValidity in ListDataValueReader.
func (r testListDataValueReader) ReadElementValidity(row, column int) bool {
	return r.values[row][column].Valid
}

// GetElementLength implements GetElementLength in ListDataValueReader.
func (r testListDataValueReader) GetElementLength(row int) int {
	return len(r.values[row])
}

// we use list.LiveVectorParty as the return struct as this vector party is easier to construct.
func (rvp *rawListVectorParty) toVectorParty() (memCom.VectorParty, error) {
	dataType := memCom.DataTypeFromString(rvp.DataType)
	if dataType == memCom.Unknown {
		return nil, utils.StackError(nil,
			"Unknown DataType when reading vector from file",
		)
	}

	if len(rvp.Values) != rvp.Length {
		return nil, utils.StackError(nil,
			"List values length %d is not as expected %d",
			len(rvp.Values),
			rvp.Length,
		)
	}

	vp := NewLiveVectorParty(rvp.Length, memCom.GetElementDataType(dataType), nil)
	vp.Allocate(false)

	for i, row := range rvp.Values {
		val, err := memCom.ValueFromString(row, dataType)
		if err != nil {
			return nil, utils.StackError(err,
				"Unable to parse value from string %s for data type %d", row, dataType)
		}

		vp.SetDataValue(i, val, memstore.IgnoreCount)
	}

	return vp, nil
}

func GetFactory() TestFactoryT {
	return testFactory
}

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

package broker

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	common2 "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore/common"
)

var _ = ginkgo.Describe("broker schema mutator", func() {
	testTable := common.Table{
		Name:    "t1",
		Columns: []common.Column{{Name: "c1", Type: "Uint32"}},
	}

	testTableOneMoreCol := common.Table{
		Name:    "t1",
		Columns: []common.Column{{Name: "c1", Type: "Uint32"}, {Name: "c2", Type: "SmallEnum"}},
	}

	testTableColDeleted := common.Table{
		Name:    "t1",
		Columns: []common.Column{{Name: "c1", Type: "Uint32"}, {Name: "c2", Type: "SmallEnum", Deleted: true}},
	}

	ginkgo.It("should work", func() {
		mutator := NewBrokerSchemaMutator()
		assertTableListLen(mutator, 0)

		err := mutator.CreateTable(&testTable)
		Ω(err).Should(BeNil())

		assertTableListLen(mutator, 1)

		t, err := mutator.GetTable("t1")
		Ω(err).Should(BeNil())
		Ω(*t).Should(Equal(testTable))

		err = mutator.UpdateTable(testTableOneMoreCol)
		Ω(err).Should(BeNil())

		t, err = mutator.GetTable("t1")
		Ω(err).Should(BeNil())
		Ω(*t).Should(Equal(testTableOneMoreCol))

		var ts *common2.TableSchema
		ts, err = mutator.GetSchema("t1")
		Ω(err).Should(BeNil())
		tsExpected := common2.NewTableSchema(&testTableOneMoreCol)
		Ω(ts).Should(Equal(tsExpected))

		err = mutator.UpdateEnum("t1", "c2", []string{"foo", "bar"})
		Ω(err).Should(BeNil())

		ts, err = mutator.GetSchema("t1")
		Ω(err).Should(BeNil())
		tsExpected.EnumDicts = map[string]common2.EnumDict{
			"c2": {
				Capacity: 256,
				Dict: map[string]int{
					"bar": 1,
					"foo": 0,
				},
				ReverseDict: []string{"foo", "bar"},
			},
		}
		Ω(ts).Should(Equal(tsExpected))

		err = mutator.DeleteColumn("t1", "bla")
		Ω(err.Error()).Should(ContainSubstring("not found"))

		err = mutator.DeleteColumn("t1", "c2")
		Ω(err).Should(BeNil())

		t, err = mutator.GetTable("t1")
		Ω(err).Should(BeNil())
		Ω(*t).Should(Equal(testTableColDeleted))

		err = mutator.DeleteTable("t1")
		Ω(err).Should(BeNil())
		assertTableListLen(mutator, 0)
	})

})

func assertTableListLen(mutator *BrokerSchemaMutator, length int) {
	tables, err := mutator.ListTables()
	Ω(err).Should(BeNil())
	Ω(tables).Should(HaveLen(length))
}

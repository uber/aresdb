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
		Name: "t1",
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

		tschema, err := mutator.GetSchema("t1")
		Ω(err).Should(BeNil())
		Ω(tschema).Should(Equal(common2.NewTableSchema(&testTable)))

		err = mutator.DeleteTable("t1")
		Ω(err).Should(BeNil())
		assertTableListLen(mutator, 0)
	})

})

func assertTableListLen(mutator *BrokerSchemaMutator, length int) {
	tables, err := mutator.ListTables()
	Ω(err).Should(BeNil())
	Ω(tables).Should(HaveLen(length))

	tableSchemas := mutator.GetSchemas()
	Ω(tableSchemas).Should(HaveLen(length))
}

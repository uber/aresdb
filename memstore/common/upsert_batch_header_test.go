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
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore/vectors"
)

var _ = ginkgo.Describe("upsert batch header tests", func() {
	ginkgo.It("upsert batch column mode", func() {
		header := UpsertBatchHeader{
			modeVector: []byte{0},
		}
		columnMode := vectors.AllValuesDefault
		columnUpdateMode := UpdateOverwriteNotNull
		header.WriteColumnFlag(columnMode, columnUpdateMode, 0)
		newMode, newUpdateMode, err := header.ReadColumnFlag(0)
		Ω(err).Should(BeNil())
		Ω(newMode).Should(Equal(columnMode))
		Ω(newUpdateMode).Should(Equal(columnUpdateMode))

		columnMode = vectors.HasNullVector
		columnUpdateMode = UpdateWithMax
		header.WriteColumnFlag(columnMode, columnUpdateMode, 0)
		newMode, newUpdateMode, err = header.ReadColumnFlag(0)
		Ω(err).Should(BeNil())
		Ω(newMode).Should(Equal(columnMode))
		Ω(newUpdateMode).Should(Equal(columnUpdateMode))

		columnMode = vectors.HasNullVector
		columnUpdateMode = MaxColumnUpdateMode
		header.WriteColumnFlag(columnMode, columnUpdateMode, 0)
		newMode, newUpdateMode, err = header.ReadColumnFlag(0)
		Ω(err).ShouldNot(BeNil())
		Ω(newMode).Should(Equal(columnMode))
		Ω(newUpdateMode).Should(Equal(columnUpdateMode))

		columnMode = vectors.MaxColumnMode
		columnUpdateMode = UpdateWithMax
		header.WriteColumnFlag(columnMode, columnUpdateMode, 0)
		newMode, newUpdateMode, err = header.ReadColumnFlag(0)
		Ω(err).ShouldNot(BeNil())
		Ω(newMode).Should(Equal(columnMode))
		Ω(newUpdateMode).Should(Equal(columnUpdateMode))
	})

	ginkgo.It("upsert batch column id", func() {
		header := UpsertBatchHeader{
			idVector: []byte{0, 0},
		}
		err := header.WriteColumnID(10, 0)
		Ω(err).Should(BeNil())

		id, err := header.ReadColumnID(0)
		Ω(err).Should(BeNil())
		Ω(id).Should(Equal(10))
	})

	ginkgo.It("upsert batch column type", func() {
		header := UpsertBatchHeader{
			typeVector: []byte{0, 0, 0, 0},
		}
		for _, t := range []DataType{Uint8, Uint16, Uint32, Int8, Int16, Int32, Int64, UUID, GeoPoint, GeoShape} {
			err := header.WriteColumnType(t, 0)
			Ω(err).Should(BeNil())
			dataType, err := header.ReadColumnType(0)
			Ω(err).Should(BeNil())
			Ω(dataType).Should(Equal(t))
		}
	})

	ginkgo.It("upsert batch column offset", func() {
		header := UpsertBatchHeader{
			offsetVector: []byte{0, 0, 0, 0},
		}
		err := header.WriteColumnOffset(10, 0)
		Ω(err).Should(BeNil())

		offset, err := header.ReadColumnOffset(0)
		Ω(err).Should(BeNil())
		Ω(offset).Should(Equal(10))
	})
})

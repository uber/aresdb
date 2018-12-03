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

package memstore

import (
	"encoding/hex"
	"strings"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memutils"
	"github.com/uber/aresdb/utils"
	"time"
)

var _ = ginkgo.Describe("upsert batch", func() {
	now := time.Unix(10, 0)
	ginkgo.BeforeEach(func() {
		utils.SetCurrentTime(now)
	})

	ginkgo.AfterEach(func() {
		utils.ResetClockImplementation()
	})

	ginkgo.It("works for empty batch", func() {
		builder := common.NewUpsertBatchBuilder()
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err := NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())
		Ω(batch.ArrivalTime).Should(Equal(uint32(now.Unix())))
		_, err = batch.GetColumnID(0)
		Ω(err).ShouldNot(BeNil())
		_, _, err = batch.GetValue(0, 0)
		Ω(err).ShouldNot(BeNil())
		_, _, err = batch.GetBool(0, 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for empty row", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(123, common.Uint8)
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err := NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())
		Ω(batch.ArrivalTime).Should(Equal(uint32(now.Unix())))
		columnID, err := batch.GetColumnID(0)
		Ω(err).Should(BeNil())
		Ω(columnID).Should(Equal(123))
		_, _, err = batch.GetValue(0, 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for empty column", func() {
		builder := common.NewUpsertBatchBuilder()
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err := NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())
		Ω(batch.ArrivalTime).Should(Equal(uint32(now.Unix())))
		_, err = batch.GetColumnID(0)
		Ω(err).ShouldNot(BeNil())
		_, _, err = batch.GetValue(0, 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for one row, one col, no value", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(123, common.Uint8)
		Ω(err).Should(BeNil())
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err := NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())

		// Read column id.
		columnID, err := batch.GetColumnID(0)
		Ω(err).Should(BeNil())
		Ω(columnID).Should(Equal(123))

		// Read the only value which should be null.
		_, valid, err := batch.GetValue(0, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))
	})

	ginkgo.It("works for one row, one col, one value", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(123, common.Uint8)
		builder.SetValue(0, 0, uint8(135))
		utils.SetCurrentTime(time.Unix(10, 0))
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err := NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())

		// Read column id.
		Ω(batch.NumRows).Should(Equal(1))
		columnID, err := batch.GetColumnID(0)
		Ω(err).Should(BeNil())
		Ω(columnID).Should(Equal(123))

		// Read the only value.
		value, valid, err := batch.GetValue(0, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*uint8)(value)).Should(Equal(uint8(135)))

		// Remove the last row.
		builder.RemoveRow()
		builder.RemoveRow()

		buffer, err = builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err = NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())

		// Read column id.
		Ω(batch.NumRows).Should(Equal(0))
		utils.ResetClockImplementation()
	})

	ginkgo.It("reset row works", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(123, common.Uint8)
		builder.SetValue(0, 0, uint8(135))
		builder.ResetRows()
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err := NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())

		// Read column id.
		columnID, err := batch.GetColumnID(0)
		Ω(err).Should(BeNil())
		Ω(columnID).Should(Equal(123))

		// Read the only value.
		_, _, err = batch.GetValue(0, 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("raises error when setting wrong value type", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddRow()
		builder.AddColumn(123, common.Uint8)
		err := builder.SetValue(0, 0, "a")
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("raises error with corrupted input", func() {
		buffer := []byte{10, 0, 0, 0, 10, 0, 0, 0}
		_, err := NewUpsertBatch(buffer)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("last value wins", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddRow()
		builder.AddRow()
		builder.AddColumn(123, common.Bool)
		err := builder.SetValue(0, 0, false)
		Ω(err).Should(BeNil())

		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err := NewUpsertBatch(buffer)
		value, valid, err := batch.GetBool(0, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(value).Should(Equal(false))

		err = builder.SetValue(0, 0, true)
		Ω(err).Should(BeNil())
		err = builder.SetValue(1, 0, false)
		Ω(err).Should(BeNil())

		buffer, err = builder.ToByteArray()
		Ω(err).Should(BeNil())
		batch, err = NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())
		value, valid, err = batch.GetBool(0, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(value).Should(Equal(true))

		err = builder.SetValue(0, 0, nil)
		Ω(err).Should(BeNil())

		buffer, err = builder.ToByteArray()
		Ω(err).Should(BeNil())
		batch, err = NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())
		value, valid, err = batch.GetBool(0, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))
	})

	ginkgo.It("works for bool type", func() {
		builder := common.NewUpsertBatchBuilder()

		// All null bools.
		err := builder.AddColumn(123, common.Bool)
		Ω(err).Should(BeNil())

		// Half valid bools.
		err = builder.AddColumn(456, common.Bool)
		builder.AddRow()
		Ω(err).Should(BeNil())
		err = builder.SetValue(0, 1, true)

		// All valid bools.
		err = builder.AddColumn(789, common.Bool)
		builder.AddRow()
		Ω(err).Should(BeNil())
		err = builder.SetValue(0, 2, true)
		Ω(err).Should(BeNil())
		err = builder.SetValue(1, 2, false)
		Ω(err).Should(BeNil())

		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err := NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())
		Ω(batch).ShouldNot(BeNil())

		// Read column id.
		columnID, err := batch.GetColumnID(0)
		Ω(err).Should(BeNil())
		Ω(columnID).Should(Equal(123))
		columnID, err = batch.GetColumnID(1)
		Ω(err).Should(BeNil())
		Ω(columnID).Should(Equal(456))
		columnID, err = batch.GetColumnID(2)
		Ω(err).Should(BeNil())
		Ω(columnID).Should(Equal(789))

		// Read the values.
		value, valid, err := batch.GetBool(0, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))

		value, valid, err = batch.GetBool(1, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))

		value, valid, err = batch.GetBool(0, 1)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(value).Should(Equal(true))

		value, valid, err = batch.GetBool(1, 1)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))

		value, valid, err = batch.GetBool(0, 2)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(value).Should(Equal(true))

		value, valid, err = batch.GetBool(1, 2)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(value).Should(Equal(false))
	})

	ginkgo.It("works for mixed types", func() {
		builder := common.NewUpsertBatchBuilder()

		builder.AddColumn(0, common.Bool)
		builder.AddColumn(1, common.Int8)
		builder.AddColumn(2, common.Uint8)
		builder.AddColumn(3, common.Int16)
		builder.AddColumn(4, common.Uint16)
		builder.AddColumn(5, common.Int32)
		builder.AddColumn(6, common.Uint32)
		builder.AddColumn(7, common.Float32)
		builder.AddColumn(8, common.SmallEnum)
		builder.AddColumn(9, common.BigEnum)
		builder.AddColumn(10, common.UUID)
		builder.AddRow()
		builder.AddRow()

		builder.SetValue(0, 0, nil)
		builder.SetValue(1, 0, true)

		builder.SetValue(0, 1, int8(-123))
		builder.SetValue(1, 1, nil)

		builder.SetValue(0, 2, uint8(234))
		builder.SetValue(1, 2, nil)

		builder.SetValue(0, 3, int16(7891))
		builder.SetValue(1, 3, nil)

		builder.SetValue(0, 4, nil)
		builder.SetValue(1, 4, uint16(5678))

		builder.SetValue(0, 5, int32(65536))
		builder.SetValue(1, 5, nil)

		builder.SetValue(0, 6, uint32(12345))
		builder.SetValue(1, 6, uint32(54321))

		builder.SetValue(0, 7, float32(-3.1415))
		builder.SetValue(1, 7, float32(3.1416))

		builder.SetValue(0, 8, uint8(135))
		builder.SetValue(1, 8, nil)

		builder.SetValue(0, 9, nil)
		builder.SetValue(1, 9, uint16(6553))

		builder.SetValue(0, 10, nil)
		builder.SetValue(1, 10, [2]uint64{123, 456})

		// All valid bools.
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())

		batch, err := NewUpsertBatch(buffer)
		Ω(err).Should(BeNil())

		valueBool, valid, err := batch.GetBool(0, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))
		valueBool, valid, err = batch.GetBool(1, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(valueBool).Should(Equal(true))

		value, valid, err := batch.GetValue(0, 1)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*int8)(value)).Should(Equal(int8(-123)))
		value, valid, err = batch.GetValue(1, 1)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))

		value, valid, err = batch.GetValue(0, 2)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*uint8)(value)).Should(Equal(uint8(234)))
		value, valid, err = batch.GetValue(1, 2)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))

		value, valid, err = batch.GetValue(0, 3)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*int16)(value)).Should(Equal(int16(7891)))
		value, valid, err = batch.GetValue(1, 3)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))

		value, valid, err = batch.GetValue(0, 4)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))
		value, valid, err = batch.GetValue(1, 4)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*uint16)(value)).Should(Equal(uint16(5678)))

		value, valid, err = batch.GetValue(0, 5)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*int32)(value)).Should(Equal(int32(65536)))
		value, valid, err = batch.GetValue(1, 5)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))

		value, valid, err = batch.GetValue(0, 6)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*uint32)(value)).Should(Equal(uint32(12345)))
		value, valid, err = batch.GetValue(1, 6)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*uint32)(value)).Should(Equal(uint32(54321)))

		value, valid, err = batch.GetValue(0, 7)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*float32)(value)).Should(Equal(float32(-3.1415)))
		value, valid, err = batch.GetValue(1, 7)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*float32)(value)).Should(Equal(float32(3.1416)))

		value, valid, err = batch.GetValue(0, 8)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*uint8)(value)).Should(Equal(uint8(135)))
		value, valid, err = batch.GetValue(1, 8)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))

		value, valid, err = batch.GetValue(0, 9)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))
		value, valid, err = batch.GetValue(1, 9)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*uint16)(value)).Should(Equal(uint16(6553)))

		value, valid, err = batch.GetValue(0, 10)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(false))
		value, valid, err = batch.GetValue(1, 10)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*uint64)(value)).Should(Equal(uint64(123)))
		Ω(*(*uint64)(memutils.MemAccess(value, 8))).Should(Equal(uint64(456)))
	})

	ginkgo.It("Test batch extra bytes", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Float32)
		builder.AddColumn(1, common.Uint32)
		builder.AddColumn(2, common.Float32)
		builder.AddColumn(3, common.UUID)
		uuidStr := "fbcc47fa-e635-412e-a882-4dff843dbd87"
		uuidBytes, _ := hex.DecodeString(strings.Replace(uuidStr, "-", "", -1))
		for r := 0; r < 416; r++ {
			builder.AddRow()
			builder.SetValue(r, 0, 1.2)
			builder.SetValue(r, 1, 123123)
			builder.SetValue(r, 2, 0.9)
			builder.SetValue(r, 3, uuidBytes)
		}

		upsertBatchBytes, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(upsertBatchBytes)

		dv, _ := upsertBatch.GetDataValue(0, 3)
		Ω(dv.ConvertToHumanReadable(common.UUID)).Should(Equal(uuidStr))
	})

	ginkgo.It("Test ExtractBackfillBatch", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Float32)
		builder.AddColumn(1, common.Bool)
		builder.AddColumn(2, common.UUID)
		uuidStr := "fbcc47fa-e635-412e-a882-4dff843dbd87"
		uuidBytes, _ := hex.DecodeString(strings.Replace(uuidStr, "-", "", -1))

		builder.AddRow()
		builder.SetValue(0, 0, 1.1)
		builder.SetValue(0, 1, true)
		builder.SetValue(0, 2, uuidBytes)

		builder.AddRow()
		builder.SetValue(1, 0, 1.2)
		builder.SetValue(1, 1, false)
		builder.SetValue(1, 2, uuidBytes)

		builder.AddRow()
		builder.SetValue(2, 0, 1.3)
		builder.SetValue(2, 1, true)
		builder.SetValue(2, 2, uuidBytes)

		upsertBatchBytes, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(upsertBatchBytes)
		newBatch := upsertBatch.ExtractBackfillBatch([]int{1})

		Ω(newBatch.NumRows).Should(Equal(1))
		dv, _ := newBatch.GetDataValue(0, 2)
		Ω(dv.ConvertToHumanReadable(common.UUID)).Should(Equal(uuidStr))
		dv, _ = newBatch.GetDataValue(0, 0)
		Ω(dv.ConvertToHumanReadable(common.Float32)).Should(Equal(float32(1.2)))
		dv, _ = newBatch.GetDataValue(0, 1)
		Ω(dv.ConvertToHumanReadable(common.Bool)).Should(Equal(false))
	})

	ginkgo.It("Test ExtractBackfillBatch remove invalid columns", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Float32)
		builder.AddColumnWithUpdateMode(1, common.Int16, common.UpdateWithAddition)
		builder.AddColumn(2, common.Bool)
		builder.AddColumn(3, common.UUID)

		uuidStr := "fbcc47fa-e635-412e-a882-4dff843dbd87"
		uuidBytes, _ := hex.DecodeString(strings.Replace(uuidStr, "-", "", -1))

		builder.AddRow()
		builder.SetValue(0, 0, 1.1)
		builder.SetValue(0, 1, 10)
		builder.SetValue(0, 2, true)
		builder.SetValue(0, 3, uuidBytes)

		builder.AddRow()
		builder.SetValue(1, 0, 1.2)
		builder.SetValue(1, 1, 20)
		builder.SetValue(1, 2, false)
		builder.SetValue(1, 3, uuidBytes)

		builder.AddRow()
		builder.SetValue(2, 0, 1.3)
		builder.SetValue(2, 1, 30)
		builder.SetValue(2, 2, true)
		builder.SetValue(2, 3, uuidBytes)

		upsertBatchBytes, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(upsertBatchBytes)
		newBatch := upsertBatch.ExtractBackfillBatch([]int{1})

		Ω(newBatch.NumRows).Should(Equal(1))
		dv, _ := newBatch.GetDataValue(0, 0)
		Ω(dv.ConvertToHumanReadable(common.Float32)).Should(Equal(float32(1.2)))
		dv, _ = newBatch.GetDataValue(0, 1)
		Ω(dv.ConvertToHumanReadable(common.Bool)).Should(Equal(false))
		dv, _ = newBatch.GetDataValue(0, 2)
		Ω(dv.ConvertToHumanReadable(common.UUID)).Should(Equal(uuidStr))
		Ω(upsertBatch.NumColumns).Should(Equal(4))
		Ω(newBatch.NumColumns).Should(Equal(3))
	})

	ginkgo.It("works for geoshape", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint32)
		builder.AddColumn(1, common.GeoShape)
		builder.AddColumn(2, common.Bool)

		builder.AddRow()
		builder.SetValue(0, 0, 2)
		builder.SetValue(0, 1, "POLYGON((-180.0 90.0, -180.0 90.0))")
		builder.SetValue(0, 2, true)

		builder.AddRow()
		builder.SetValue(1, 0, nil)
		builder.SetValue(1, 1, nil)
		builder.SetValue(1, 2, nil)

		upsertBatchBytes, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(upsertBatchBytes)

		// first row should have value
		value, err := upsertBatch.GetDataValue(0, 0)
		Ω(err).Should(BeNil())
		Ω(value).ShouldNot(BeNil())
		Ω(value.Valid).Should(BeTrue())
		Ω(*(*uint32)(value.OtherVal)).Should(Equal(uint32(2)))

		value, err = upsertBatch.GetDataValue(0, 1)
		Ω(err).Should(BeNil())
		Ω(value).ShouldNot(BeNil())
		expectedShape := &common.GeoShapeGo{
			Polygons: [][]common.GeoPointGo{
				{
					{
						90.0,
						-180.0,
					},
					{
						90.0,
						-180.0,
					},
				},
			},
		}
		Ω(value.Valid).Should(BeTrue())
		Ω(value.GoVal).Should(Equal(expectedShape))

		value, err = upsertBatch.GetDataValue(0, 2)
		Ω(err).Should(BeNil())
		Ω(value).ShouldNot(BeNil())
		Ω(value.Valid).Should(BeTrue())
		Ω(value.BoolVal).Should(BeTrue())

		// second row should be all nil
		value, err = upsertBatch.GetDataValue(1, 0)
		Ω(err).Should(BeNil())
		Ω(value).ShouldNot(BeNil())
		Ω(value.Valid).Should(BeFalse())

		value, err = upsertBatch.GetDataValue(1, 1)
		Ω(err).Should(BeNil())
		Ω(value).ShouldNot(BeNil())
		Ω(value.Valid).Should(BeFalse())

		value, err = upsertBatch.GetDataValue(1, 2)
		Ω(err).Should(BeNil())
		Ω(value).ShouldNot(BeNil())
		Ω(value.Valid).Should(BeFalse())
	})
})

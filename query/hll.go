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

package query

import (
	"github.com/uber/aresdb/cgoutils"
	memCom "github.com/uber/aresdb/memstore/common"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/utils"
	"math"
	"time"
	"unsafe"
)

// SerializeHLL allocates buffer based on the metadata and then serializes hll data into the buffer.
func (qc *AQLQueryContext) SerializeHLL(dataTypes []memCom.DataType,
	enumDicts map[int][]string, timeDimensions []int) ([]byte, error) {
	oopkContext := qc.OOPK
	paddedRawDimValuesVectorLength := (uint32(queryCom.DimValResVectorSize(oopkContext.ResultSize, oopkContext.NumDimsPerDimWidth)) + 7) / 8 * 8
	paddedCountLength := uint32(2*oopkContext.ResultSize+7) / 8 * 8
	paddedHLLVectorLength := (qc.OOPK.hllVectorSize + 7) / 8 * 8
	builder := queryCom.HLLDataWriter{
		HLLData: queryCom.HLLData{
			ResultSize:                     uint32(oopkContext.ResultSize),
			NumDimsPerDimWidth:             oopkContext.NumDimsPerDimWidth,
			DimIndexes:                     oopkContext.DimensionVectorIndex,
			DataTypes:                      dataTypes,
			EnumDicts:                      enumDicts,
			PaddedRawDimValuesVectorLength: paddedRawDimValuesVectorLength,
			PaddedHLLVectorLength:          paddedHLLVectorLength,
		},
	}

	headerSize, totalSize := builder.CalculateSizes()
	builder.Buffer = make([]byte, totalSize)
	if err := builder.SerializeHeader(); err != nil {
		return nil, err
	}

	// Copy dim values vector from device.
	dimVectorH := unsafe.Pointer(&builder.Buffer[headerSize])
	asyncCopyDimensionVector(dimVectorH, oopkContext.currentBatch.dimensionVectorD[0].getPointer(),
		oopkContext.ResultSize, 0, oopkContext.NumDimsPerDimWidth, oopkContext.ResultSize, oopkContext.currentBatch.resultCapacity,
		cgoutils.AsyncCopyDeviceToHost, qc.cudaStreams[0], qc.Device)

	cgoutils.AsyncCopyDeviceToHost(unsafe.Pointer(&builder.Buffer[headerSize+paddedRawDimValuesVectorLength]),
		oopkContext.hllDimRegIDCountD.getPointer(), oopkContext.ResultSize*2, qc.cudaStreams[0], qc.Device)

	cgoutils.AsyncCopyDeviceToHost(unsafe.Pointer(&builder.Buffer[headerSize+paddedRawDimValuesVectorLength+paddedCountLength]),
		oopkContext.hllVectorD.getPointer(), int(qc.OOPK.hllVectorSize), qc.cudaStreams[0], qc.Device)
	cgoutils.WaitForCudaStream(qc.cudaStreams[0], qc.Device)

	// Fix time dimension by substracting the timezone.
	if len(timeDimensions) > 0 && qc.fixedTimezone.String() != time.UTC.String() {
		// length is equal to length of timeDimensions
		dimPtrs := make([][2]unsafe.Pointer, len(timeDimensions))

		for i := 0; i < len(timeDimensions); i++ {
			dimIndex := timeDimensions[i]
			dimVectorIndex := qc.OOPK.DimensionVectorIndex[dimIndex]
			valueOffset, nullOffset := queryCom.GetDimensionStartOffsets(oopkContext.NumDimsPerDimWidth, dimVectorIndex, int(qc.OOPK.ResultSize))
			dimPtrs[i] = [2]unsafe.Pointer{utils.MemAccess(dimVectorH, valueOffset), utils.MemAccess(dimVectorH, nullOffset)}
		}

		for rowNumber := 0; rowNumber < oopkContext.ResultSize; rowNumber++ {
			for i := 0; i < len(timeDimensions); i++ {
				valueStart, nullStart := dimPtrs[i][0], dimPtrs[i][1]
				// We don't need to do anything for null.
				if *(*uint8)(utils.MemAccess(nullStart, rowNumber)) == 0 {
					continue
				}

				valuePtr := (*uint32)(utils.MemAccess(valueStart, rowNumber*4))
				// Don't need to check type of time dimension, they should be guaranteed by AQL Compiler.

				newVal := int64(*valuePtr)
				if qc.fromTime != nil {
					_, fromOffset := qc.fromTime.Time.Zone()
					_, toOffset := qc.toTime.Time.Zone()
					newVal = utils.AdjustOffset(fromOffset, toOffset, qc.dstswitch, int64(*valuePtr))
				}

				if newVal >= math.MaxUint32 {
					newVal = math.MaxUint32
				}

				if newVal <= 0 {
					newVal = 0
				}
				*valuePtr = uint32(newVal)
			}
		}
	}

	return builder.Buffer, nil
}

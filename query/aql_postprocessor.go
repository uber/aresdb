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

// #include "time_series_aggregate.h"
import "C"

import (
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memutils"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"unsafe"
)

// Postprocess converts the internal dimension and measure vector in binary
// format to AQLQueryResult nested result format. It also translates enum
// values back to their string representations.
func (qc *AQLQueryContext) Postprocess() queryCom.AQLQueryResult {
	oopkContext := qc.OOPK
	if oopkContext.IsHLL() {
		result, err := queryCom.NewTimeSeriesHLLResult(qc.HLLQueryResult, queryCom.HLLDataHeader)
		if err != nil {
			// should never be here except bug
			qc.Error = utils.StackError(err, "failed to read hll result")
			return nil
		}
		return queryCom.ComputeHLLResult(result)
	}

	result := make(queryCom.AQLQueryResult)
	dimValues := make([]*string, len(oopkContext.Dimensions))
	dataTypes := make([]memCom.DataType, len(oopkContext.Dimensions))
	reverseDicts := make(map[int][]string)
	dimOffsets := make(map[int][2]int)

	for dimIndex, dimExpr := range oopkContext.Dimensions {
		dimVectorIndex := oopkContext.DimensionVectorIndex[dimIndex]
		valueOffset, nullOffset := queryCom.GetDimensionStartOffsets(oopkContext.NumDimsPerDimWidth, dimVectorIndex, oopkContext.ResultSize)
		dimOffsets[dimIndex] = [2]int{valueOffset, nullOffset}
		dataTypes[dimIndex], reverseDicts[dimIndex] = getDimensionDataType(dimExpr), qc.getEnumReverseDict(dimIndex, dimExpr)
	}

	var fromOffset, toOffset int
	if qc.fromTime != nil && qc.toTime != nil {
		_, fromOffset = qc.fromTime.Time.Zone()
		_, toOffset = qc.toTime.Time.Zone()
	}
	// caches time formatted time dimension values
	dimensionValueCache := make([]map[queryCom.TimeDimensionMeta]map[int64]string, len(oopkContext.Dimensions))
	for i := 0; i < oopkContext.ResultSize; i++ {
		for dimIndex := range oopkContext.Dimensions {
			offsets := dimOffsets[dimIndex]
			valueOffset, nullOffset := offsets[0], offsets[1]
			valuePtr, nullPtr := memutils.MemAccess(oopkContext.dimensionVectorH, valueOffset), memutils.MemAccess(oopkContext.dimensionVectorH, nullOffset)

			if qc.Query.Dimensions[dimIndex].isTimeDimension() && dimensionValueCache[dimIndex] == nil {
				dimensionValueCache[dimIndex] = make(map[queryCom.TimeDimensionMeta]map[int64]string)
			}

			var timeDimensionMeta *queryCom.TimeDimensionMeta

			if qc.Query.Dimensions[dimIndex].isTimeDimension() {
				timeDimensionMeta = &queryCom.TimeDimensionMeta{
					TimeBucketizer:  qc.Query.Dimensions[dimIndex].TimeBucketizer,
					TimeUnit:        qc.Query.Dimensions[dimIndex].TimeUnit,
					IsTimezoneTable: qc.timezoneTable.tableColumn != "",
					TimeZone:        qc.fixedTimezone,
					DSTSwitchTs:     qc.dstswitch,
					FromOffset:      fromOffset,
					ToOffset:        toOffset,
				}
			}

			dimValues[dimIndex] = queryCom.ReadDimension(
				valuePtr, nullPtr, i, dataTypes[dimIndex], reverseDicts[dimIndex],
				timeDimensionMeta, dimensionValueCache[dimIndex])
		}

		if qc.isNonAggregationQuery {
			values := make([]interface{}, len(dimValues))
			for index, v := range dimValues {
				if v == nil {
					values[index] = "NULL"
				} else {
					values[index] = *v
				}
			}
			result.Append(values)
		} else {
			measureBytes := oopkContext.MeasureBytes

			// For avg aggregation function, we only need to read first 4 bytes which is the average.
			if qc.OOPK.AggregateType == C.AGGR_AVG_FLOAT {
				measureBytes = 4
			}

			measureValue := readMeasure(
				memutils.MemAccess(oopkContext.measureVectorH, i*oopkContext.MeasureBytes), oopkContext.Measure,
				measureBytes)

			result.Set(dimValues, measureValue)
		}
	}

	if qc.isNonAggregationQuery {
		headers := make([]string, len(qc.Query.Dimensions))
		for i, dim := range qc.Query.Dimensions {
			headers[i] = dim.Expr
		}
		result.SetHeaders(headers)
	}
	return result
}

// PostprocessAsHLLData serializes the query result into HLLData format. It will also release the device memory after
// serialization.
func (qc *AQLQueryContext) PostprocessAsHLLData() ([]byte, error) {
	oopkContext := qc.OOPK
	if oopkContext.ResultSize == 0 {
		return []byte{}, nil
	}

	dataTypes := make([]memCom.DataType, len(oopkContext.Dimensions))
	reverseDicts := make(map[int][]string)

	var timeDimensions []int
	for dimIndex, ast := range oopkContext.Dimensions {
		dataTypes[dimIndex], reverseDicts[dimIndex] = getDimensionDataType(ast), qc.getEnumReverseDict(dimIndex, ast)
		if qc.Query.Dimensions[dimIndex].isTimeDimension() {
			timeDimensions = append(timeDimensions, dimIndex)
		}
	}

	return qc.SerializeHLL(dataTypes, reverseDicts, timeDimensions)
}

// getEnumReverseDict returns the enum reverse dict of a ast node if it's a VarRef node, otherwise it will return
// a nil slice.
func (qc *AQLQueryContext) getEnumReverseDict(dimIndex int, expression expr.Expr) []string {
	varRef, ok := expression.(*expr.VarRef)
	if ok && (varRef.DataType == memCom.SmallEnum || varRef.DataType == memCom.BigEnum) {
		return varRef.EnumReverseDict
	}

	// return validShapeUUIDs as the reverse enum dict if dimIndex match geo dimension
	if qc.OOPK.geoIntersection != nil && qc.OOPK.geoIntersection.dimIndex == dimIndex {
		return qc.OOPK.geoIntersection.validShapeUUIDs
	}

	return nil
}

// ReleaseHostResultsBuffers deletes the result buffer from host memory after postprocessing
func (qc *AQLQueryContext) ReleaseHostResultsBuffers() {
	ctx := &qc.OOPK
	memutils.HostFree(ctx.dimensionVectorH)
	ctx.dimensionVectorH = nil
	if ctx.measureVectorH != nil {
		memutils.HostFree(ctx.measureVectorH)
		ctx.measureVectorH = nil
	}

	// hllVectorD and hllDimRegIDCountD used for hll query only
	deviceFreeAndSetNil(&ctx.hllVectorD)
	deviceFreeAndSetNil(&ctx.hllDimRegIDCountD)

	// set geoIntersection to nil
	qc.OOPK.geoIntersection = nil
}

func readMeasure(measureRow unsafe.Pointer, ast expr.Expr, measureBytes int) *float64 {
	// TODO: consider converting non-zero identity values to nil.
	var result float64
	if measureBytes == 4 {
		switch ast.Type() {
		case expr.Unsigned:
			result = float64(*(*uint32)(measureRow))
		case expr.Signed, expr.Boolean:
			result = float64(*(*int32)(measureRow))
		case expr.Float:
			result = float64(*(*float32)(measureRow))
		default:
			// Should never happen
			return nil
		}
	} else if measureBytes == 8 {
		switch ast.Type() {
		case expr.Unsigned:
			result = float64(*(*uint64)(measureRow))
		case expr.Signed, expr.Boolean:
			result = float64(*(*int64)(measureRow))
		case expr.Float:
			result = *(*float64)(measureRow)
		default:
			// Should never happen.
			return nil
		}
	} else { // should never happen
		return nil
	}

	return &result
}

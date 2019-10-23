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

// #cgo LDFLAGS: -L${SRCDIR}/../lib -lalgorithm
// #include "time_series_aggregate.h"
import "C"
import (
	"github.com/uber/aresdb/utils"
	"strconv"
	"unsafe"

	"github.com/uber/aresdb/cgoutils"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
)

// DataTypeToCDataType mapps from memstore data type to c data types
var DataTypeToCDataType = map[memCom.DataType]C.enum_DataType{
	memCom.Bool:      C.Bool,
	memCom.Int8:      C.Int8,
	memCom.Uint8:     C.Uint8,
	memCom.Int16:     C.Int16,
	memCom.Uint16:    C.Uint16,
	memCom.Int32:     C.Int32,
	memCom.Int64:     C.Int64,
	memCom.Uint32:    C.Uint32,
	memCom.Float32:   C.Float32,
	memCom.SmallEnum: C.Uint8,
	memCom.BigEnum:   C.Uint16,
	memCom.GeoPoint:  C.GeoPoint,
	memCom.UUID:      C.UUID,
}

// UnaryExprTypeToCFunctorType maps from unary operator to C UnaryFunctorType
var UnaryExprTypeToCFunctorType = map[expr.Token]C.enum_UnaryFunctorType{
	expr.NOT:                 C.Not,
	expr.UNARY_MINUS:         C.Negate,
	expr.IS_NULL:             C.IsNull,
	expr.IS_NOT_NULL:         C.IsNotNull,
	expr.BITWISE_NOT:         C.BitwiseNot,
	expr.GET_WEEK_START:      C.GetWeekStart,
	expr.GET_MONTH_START:     C.GetMonthStart,
	expr.GET_QUARTER_START:   C.GetQuarterStart,
	expr.GET_YEAR_START:      C.GetYearStart,
	expr.GET_DAY_OF_MONTH:    C.GetDayOfMonth,
	expr.GET_DAY_OF_YEAR:     C.GetDayOfYear,
	expr.GET_MONTH_OF_YEAR:   C.GetMonthOfYear,
	expr.GET_QUARTER_OF_YEAR: C.GetQuarterOfYear,
	expr.GET_HLL_VALUE:       C.GetHLLValue,
	expr.ARRAY_LENGTH:        C.ArrayLength,
}

// BinaryExprTypeToCFunctorType maps from binary operator to C BinaryFunctorType
var BinaryExprTypeToCFunctorType = map[expr.Token]C.enum_BinaryFunctorType{
	expr.AND:              C.And,
	expr.OR:               C.Or,
	expr.EQ:               C.Equal,
	expr.NEQ:              C.NotEqual,
	expr.LT:               C.LessThan,
	expr.LTE:              C.LessThanOrEqual,
	expr.GT:               C.GreaterThan,
	expr.GTE:              C.GreaterThanOrEqual,
	expr.ADD:              C.Plus,
	expr.SUB:              C.Minus,
	expr.MUL:              C.Multiply,
	expr.DIV:              C.Divide,
	expr.MOD:              C.Mod,
	expr.BITWISE_AND:      C.BitwiseAnd,
	expr.BITWISE_OR:       C.BitwiseOr,
	expr.BITWISE_XOR:      C.BitwiseXor,
	expr.FLOOR:            C.Floor,
	expr.CONVERT_TZ:       C.Plus,
	expr.ARRAY_CONTAINS:   C.ArrayContains,
	expr.ARRAY_ELEMENT_AT: C.ArrayElementAt,
	// TODO: expr.BITWISE_LEFT_SHIFT ?
	// TODO: expr.BITWISE_RIGHT_SHIFT ?
}

type rootAction func(functorType uint32, stream unsafe.Pointer, device int, inputs []C.InputVector, exp expr.Expr)

func makeForeignColumnInput(columnIndex int, recordIDs unsafe.Pointer, table foreignTable, timezoneLookup unsafe.Pointer, timezoneLookupSize int) C.InputVector {
	var vector C.InputVector
	var foreignColumnVector C.ForeignColumnVector

	vpSlices := make([]C.VectorPartySlice, len(table.batches))
	var dataType memCom.DataType
	var defaultValue memCom.DataValue
	for batchIndex, batch := range table.batches {
		column := batch[columnIndex]
		dataType = column.valueType
		defaultValue = column.defaultValue
		vpSlices[batchIndex] = makeVectorPartySlice(batch[columnIndex])
	}

	foreignColumnVector.RecordIDs = (*C.RecordID)(recordIDs)
	if len(vpSlices) > 0 {
		foreignColumnVector.Batches = (*C.VectorPartySlice)(unsafe.Pointer(&vpSlices[0]))
	}
	foreignColumnVector.BaseBatchID = (C.int32_t)(memstore.BaseBatchID)
	foreignColumnVector.NumBatches = (C.int32_t)(len(table.batches))
	foreignColumnVector.NumRecordsInLastBatch = (C.int32_t)(table.numRecordsInLastBatch)
	foreignColumnVector.DataType = DataTypeToCDataType[dataType]
	foreignColumnVector.DefaultValue = makeDefaultValue(defaultValue)
	foreignColumnVector.TimezoneLookup = (*C.int16_t)(timezoneLookup)
	foreignColumnVector.TimezoneLookupSize = (C.int16_t)(timezoneLookupSize)

	*(*C.ForeignColumnVector)(unsafe.Pointer(&vector.Vector)) = foreignColumnVector
	vector.Type = C.ForeignColumnInput
	return vector
}

func makeDefaultValue(value memCom.DataValue) C.DefaultValue {
	var defaultValue C.DefaultValue
	defaultValue.HasDefault = (C.bool)(value.Valid)
	if value.Valid {
		switch value.DataType {
		case memCom.Bool:
			*(*C.bool)(unsafe.Pointer(&defaultValue.Value)) = (C.bool)(value.BoolVal)
		case memCom.Int8:
			*(*C.int32_t)(unsafe.Pointer(&defaultValue.Value)) = (C.int32_t)(*(*int8)(value.OtherVal))
		case memCom.Int16:
			*(*C.int32_t)(unsafe.Pointer(&defaultValue.Value)) = (C.int32_t)(*(*int16)(value.OtherVal))
		case memCom.Int32:
			*(*C.int32_t)(unsafe.Pointer(&defaultValue.Value)) = (C.int32_t)(*(*int32)(value.OtherVal))
		case memCom.SmallEnum:
			fallthrough
		case memCom.Uint8:
			*(*C.uint32_t)(unsafe.Pointer(&defaultValue.Value)) = (C.uint32_t)(*(*uint8)(value.OtherVal))
		case memCom.BigEnum:
			fallthrough
		case memCom.Uint16:
			*(*C.uint32_t)(unsafe.Pointer(&defaultValue.Value)) = (C.uint32_t)(*(*uint16)(value.OtherVal))
		case memCom.Uint32:
			*(*C.uint32_t)(unsafe.Pointer(&defaultValue.Value)) = (C.uint32_t)(*(*uint32)(value.OtherVal))
		case memCom.Float32:
			*(*C.float)(unsafe.Pointer(&defaultValue.Value)) = (C.float)(*(*float32)(value.OtherVal))
		case memCom.Int64:
			*(*C.int64_t)(unsafe.Pointer(&defaultValue.Value)) = (C.int64_t)(*(*int64)(value.OtherVal))
		case memCom.GeoPoint:
			*(*C.GeoPointT)(unsafe.Pointer(&defaultValue.Value)) = *(*C.GeoPointT)(value.OtherVal)
		case memCom.UUID:
			*(*C.UUIDT)(unsafe.Pointer(&defaultValue.Value)) = *(*C.UUIDT)(value.OtherVal)
		default:
			// Otherwise it's the default value type we don't support yet, setting it to null to be safe.
			defaultValue.HasDefault = false
		}
	}
	return defaultValue
}

func makeVectorPartySlice(column deviceVectorPartySlice) C.VectorPartySlice {
	var vpSlice C.VectorPartySlice
	var basePtr unsafe.Pointer
	var startingIndex int
	var nullsOffset uint32
	var valuesOffset uint32

	if !column.counts.isNull() {
		basePtr = utils.MemAccess(column.counts.getPointer(), column.countStartIndex*4)
	}

	if !column.nulls.isNull() {
		startingIndex = column.nullStartIndex % 8
		nulls := utils.MemAccess(column.nulls.getPointer(), column.nullStartIndex/8)
		if basePtr == nil {
			basePtr = nulls
		} else {
			nullsOffset = uint32(utils.MemDist(nulls, basePtr))
		}
	}

	if !column.values.isNull() {
		values := utils.MemAccess(column.values.getPointer(),
			column.valueStartIndex*memCom.DataTypeBits(column.valueType)/8)
		if basePtr == nil {
			basePtr = values
		} else {
			valuesOffset = uint32(utils.MemDist(values, basePtr))
		}
	}

	vpSlice.BasePtr = (*C.uint8_t)(basePtr)
	vpSlice.NullsOffset = (C.uint32_t)(nullsOffset)
	vpSlice.ValuesOffset = (C.uint32_t)(valuesOffset)
	vpSlice.StartingIndex = (C.uint8_t)(startingIndex)

	vpSlice.Length = (C.uint32_t)(column.length)
	vpSlice.DataType = DataTypeToCDataType[column.valueType]
	vpSlice.DefaultValue = makeDefaultValue(column.defaultValue)
	return vpSlice
}

func makeArrayVectorPartySlice(column deviceVectorPartySlice) C.ArrayVectorPartySlice {
	var vpSlice C.ArrayVectorPartySlice
	vpSlice.OffsetLengthVector = (*C.uint8_t)(column.basePtr.getPointer())
	vpSlice.ValueOffsetAdj = (C.uint32_t)(column.valueOffsetAdjust)
	vpSlice.Length = (C.uint32_t)(column.length)
	vpSlice.DataType = DataTypeToCDataType[memCom.GetElementDataType(column.valueType)]
	return vpSlice
}

func makeVectorPartySliceInput(column deviceVectorPartySlice) C.InputVector {
	if memCom.IsArrayType(column.valueType) {
		return makeArrayVectorPartySliceInput(column)
	}
	var vector C.InputVector
	*(*C.VectorPartySlice)(unsafe.Pointer(&vector.Vector)) = makeVectorPartySlice(column)
	vector.Type = C.VectorPartyInput
	return vector
}

func makeArrayVectorPartySliceInput(column deviceVectorPartySlice) C.InputVector {
	var vector C.InputVector
	*(*C.ArrayVectorPartySlice)(unsafe.Pointer(&vector.Vector)) = makeArrayVectorPartySlice(column)
	vector.Type = C.ArrayVectorPartyInput
	return vector
}

func makeConstantInput(val interface{}, isValid bool) C.InputVector {
	var constVector C.ConstantVector
	constVector.IsValid = C.bool(isValid)

	switch val.(type) {
	case float64, float32:
		floatVal := val.(float64)
		*(*C.float)(unsafe.Pointer(&constVector.Value)) = C.float(floatVal)
		constVector.DataType = C.ConstFloat
	case *expr.GeopointLiteral:
		geopoint := val.(*expr.GeopointLiteral).Val
		*(*C.GeoPointT)(unsafe.Pointer(&constVector.Value)) = *(*C.GeoPointT)(unsafe.Pointer(&geopoint[0]))
		constVector.DataType = C.ConstGeoPoint
	case *expr.UUIDLiteral:
		uuidVal := val.(*expr.UUIDLiteral).Val
		*(*C.UUIDT)(unsafe.Pointer(&constVector.Value)) = *(*C.UUIDT)(unsafe.Pointer(&uuidVal[0]))
		constVector.DataType = C.ConstUUID
	case *expr.NumberLiteral:
		t := val.(*expr.NumberLiteral)
		if t.Type() == expr.Float {
			*(*C.float)(unsafe.Pointer(&constVector.Value)) = C.float(t.Val)
			constVector.DataType = C.ConstFloat
		} else {
			*(*C.int32_t)(unsafe.Pointer(&constVector.Value)) = C.int32_t(t.Int)
			constVector.DataType = C.ConstInt
		}
	default:
		intVal := val.(int)
		*(*C.int32_t)(unsafe.Pointer(&constVector.Value)) = C.int32_t(intVal)
		constVector.DataType = C.ConstInt
	}

	var vector C.InputVector
	*(*C.ConstantVector)(unsafe.Pointer(&vector.Vector)) = constVector
	vector.Type = C.ConstantInput
	return vector
}

func makeScratchSpaceInput(values unsafe.Pointer, nulls unsafe.Pointer, dataType C.enum_DataType) C.InputVector {
	var scratchSpaceVector C.ScratchSpaceVector
	scratchSpaceVector.Values = (*C.uint8_t)(values)
	scratchSpaceVector.NullsOffset = (C.uint32_t)(utils.MemDist(nulls, values))
	scratchSpaceVector.DataType = dataType

	var vector C.InputVector
	*(*C.ScratchSpaceVector)(unsafe.Pointer(&vector.Vector)) = scratchSpaceVector
	vector.Type = C.ScratchSpaceInput

	return vector
}

func makeMeasureVectorOutput(measureVector unsafe.Pointer, outputDataType C.enum_DataType, aggFunc C.enum_AggregateFunction) C.OutputVector {
	var measureOutputVector C.MeasureOutputVector

	measureOutputVector.Values = (*C.uint32_t)(measureVector)
	measureOutputVector.DataType = outputDataType
	measureOutputVector.AggFunc = aggFunc

	var vector C.OutputVector
	*(*C.MeasureOutputVector)(unsafe.Pointer(&vector.Vector)) = measureOutputVector
	vector.Type = C.MeasureOutput
	return vector
}

func makeDimensionVectorOutput(dimensionVector unsafe.Pointer, valueOffset, nullOffset int, dataType C.enum_DataType) C.OutputVector {
	var dimensionOutputVector C.DimensionOutputVector

	dimensionOutputVector.DimValues = (*C.uint8_t)(utils.MemAccess(dimensionVector, valueOffset))
	dimensionOutputVector.DimNulls = (*C.uint8_t)(utils.MemAccess(dimensionVector, nullOffset))
	dimensionOutputVector.DataType = dataType

	var vector C.OutputVector
	*(*C.DimensionOutputVector)(unsafe.Pointer(&vector.Vector)) = dimensionOutputVector
	vector.Type = C.DimensionOutput
	return vector
}

func makeScratchSpaceOutput(values unsafe.Pointer, nulls unsafe.Pointer, dataType C.enum_DataType) C.OutputVector {
	var scratchSpaceVector C.ScratchSpaceVector
	scratchSpaceVector.Values = (*C.uint8_t)(values)
	scratchSpaceVector.NullsOffset = (C.uint32_t)(utils.MemDist(nulls, values))
	scratchSpaceVector.DataType = dataType

	var vector C.OutputVector
	*(*C.ScratchSpaceVector)(unsafe.Pointer(&vector.Vector)) = scratchSpaceVector
	vector.Type = C.ScratchSpaceOutput

	return vector
}

func makeDimensionVector(valueVector, hashVector, indexVector unsafe.Pointer, numDims common.DimCountsPerDimWidth, vectorCapacity int) C.DimensionVector {
	var dimensionVector C.DimensionVector
	dimensionVector.DimValues = (*C.uint8_t)(valueVector)
	dimensionVector.HashValues = (*C.uint64_t)(hashVector)
	dimensionVector.IndexVector = (*C.uint32_t)(indexVector)

	dimensionVector.VectorCapacity = (C.int)(vectorCapacity)
	for i := 0; i < len(numDims); i++ {
		dimensionVector.NumDimsPerDimWidth[i] = (C.uint8_t)(numDims[i])
	}
	return dimensionVector
}

func getOutputDataType(exprType expr.Type, outputWidthInByte int) C.enum_DataType {
	if exprType == expr.UUID {
		return C.UUID
	} else if exprType == expr.GeoPoint {
		return C.GeoPoint
	}
	if outputWidthInByte == 4 {
		switch exprType {
		case expr.Float:
			return C.Float32
		case expr.Unsigned:
			return C.Uint32
		default:
			return C.Int32
		}
	} else {
		switch exprType {
		case expr.Float:
			return C.Float64
			// For reducing the measure output iterator cardinality.
		case expr.Unsigned:
			return C.Int64
		default:
			return C.Int64
		}
	}
}

func initIndexVector(vector unsafe.Pointer, start, size int, stream unsafe.Pointer, device int) {
	C.InitIndexVector((*C.uint32_t)(vector), (C.uint32_t)(start), (C.int)(size), stream, (C.int)(device))
}

func (bc *oopkBatchContext) filterAction(functorType uint32, stream unsafe.Pointer, device int, inputs []C.InputVector, exp expr.Expr) {
	numForeignTables := len(bc.foreignTableRecordIDsD)
	// If current batch size is already 0, short circuit to avoid issuing a noop cuda call.
	if bc.size <= 0 {
		return
	}

	foreignTableRecordIDs := unsafe.Pointer(nil)
	if numForeignTables > 0 {
		foreignTableRecordIDs = unsafe.Pointer(&bc.foreignTableRecordIDsD[0].pointer)
	}
	if len(inputs) == 1 {
		bc.size = int(doCGoCall(func() C.CGoCallResHandle {
			return C.UnaryFilter(inputs[0], (*C.uint32_t)(bc.indexVectorD.getPointer()),
				(*C.uint8_t)(bc.predicateVectorD.getPointer()),
				(C.int)(bc.size), (**C.RecordID)(foreignTableRecordIDs),
				(C.int)(numForeignTables),
				(*C.uint32_t)(bc.baseCountD.getPointer()), (C.uint32_t)(bc.startRow), functorType, stream, C.int(device))
		}))
	} else if len(inputs) == 2 {
		bc.size = int(doCGoCall(func() C.CGoCallResHandle {
			return C.BinaryFilter(inputs[0], inputs[1], (*C.uint32_t)(bc.indexVectorD.getPointer()),
				(*C.uint8_t)(bc.predicateVectorD.getPointer()),
				(C.int)(bc.size), (**C.RecordID)(foreignTableRecordIDs), (C.int)(numForeignTables),
				(*C.uint32_t)(bc.baseCountD.getPointer()), (C.uint32_t)(bc.startRow), functorType, stream, C.int(device))
		}))
	}
}

func (bc *oopkBatchContext) makeWriteToMeasureVectorAction(aggFunc C.enum_AggregateFunction, outputWidthInByte int) rootAction {
	return func(functorType uint32, stream unsafe.Pointer, device int, inputs []C.InputVector, exp expr.Expr) {
		// If current batch size is already 0, short circuit to avoid issuing a noop cuda call.
		if bc.size <= 0 {
			return
		}
		measureVector := utils.MemAccess(bc.measureVectorD[0].getPointer(), bc.resultSize*outputWidthInByte)
		// write measure out to measureVectorD[1] for hll query
		if aggFunc == C.AGGR_HLL {
			measureVector = bc.measureVectorD[1].getPointer()
		}
		outputVector := makeMeasureVectorOutput(measureVector, getOutputDataType(exp.Type(), outputWidthInByte), aggFunc)

		if len(inputs) == 1 {
			doCGoCall(func() C.CGoCallResHandle {
				return C.UnaryTransform(inputs[0], outputVector, (*C.uint32_t)(bc.indexVectorD.getPointer()),
					(C.int)(bc.size), (*C.uint32_t)(bc.baseCountD.getPointer()), (C.uint32_t)(bc.startRow), functorType, stream, C.int(device))
			})
		} else if len(inputs) == 2 {
			doCGoCall(func() C.CGoCallResHandle {
				return C.BinaryTransform(inputs[0], inputs[1], outputVector,
					(*C.uint32_t)(bc.indexVectorD.getPointer()), (C.int)(bc.size), (*C.uint32_t)(bc.baseCountD.getPointer()), (C.uint32_t)(bc.startRow),
					functorType, stream, C.int(device))
			})
		}
	}
}

func (bc *oopkBatchContext) makeWriteToDimensionVectorAction(valueOffset, nullOffset, prevResultSize int) rootAction {
	return func(functorType uint32, stream unsafe.Pointer, device int, inputs []C.InputVector, exp expr.Expr) {
		// If current batch size is already 0, short circuit to avoid issuing a noop cuda call.
		if bc.size <= 0 {
			return
		}

		dataType := common.GetDimensionDataType(exp)
		dataBytes := common.GetDimensionDataBytes(exp)
		outputVector := makeDimensionVectorOutput(
			bc.dimensionVectorD[0].getPointer(),
			// move dimensionVectorD to the start position of current batch
			// dimension vector start position + bc.resultSize * dataBytes
			// null vector start position + bc.resultSize
			valueOffset+dataBytes*prevResultSize,
			nullOffset+prevResultSize,
			DataTypeToCDataType[dataType])

		if len(inputs) == 1 {
			doCGoCall(func() C.CGoCallResHandle {
				return C.UnaryTransform(inputs[0], outputVector, (*C.uint32_t)(bc.indexVectorD.getPointer()),
					(C.int)(bc.size), (*C.uint32_t)(bc.baseCountD.getPointer()), (C.uint32_t)(bc.startRow), functorType,
					stream, C.int(device))
			})
		} else if len(inputs) == 2 {
			doCGoCall(func() C.CGoCallResHandle {
				return C.BinaryTransform(inputs[0], inputs[1], outputVector,
					(*C.uint32_t)(bc.indexVectorD.getPointer()), (C.int)(bc.size),
					(*C.uint32_t)(bc.baseCountD.getPointer()), (C.uint32_t)(bc.startRow),
					functorType, stream, C.int(device))
			})
		}
	}
}

func makeCuckooHashIndex(primaryKeyData memCom.PrimaryKeyData, deviceData unsafe.Pointer) C.CuckooHashIndex {
	var cuckooHashIndex C.CuckooHashIndex
	cuckooHashIndex.buckets = (*C.uint8_t)(deviceData)
	for index, seed := range primaryKeyData.Seeds {
		cuckooHashIndex.seeds[index] = (C.uint32_t)(seed)
	}
	cuckooHashIndex.keyBytes = (C.int)(primaryKeyData.KeyBytes)
	cuckooHashIndex.numHashes = (C.int)(len(primaryKeyData.Seeds))
	cuckooHashIndex.numBuckets = (C.int)(primaryKeyData.NumBuckets)
	return cuckooHashIndex
}

func (bc *oopkBatchContext) prepareForeignRecordIDs(mainTableJoinColumnIndex int, joinTableID int, table foreignTable,
	stream unsafe.Pointer, device int) {
	// If current batch size is already 0, short circuit to avoid issuing a noop cuda call.
	if bc.size <= 0 {
		return
	}

	column := bc.columns[mainTableJoinColumnIndex]
	inputVector := makeVectorPartySliceInput(column)
	hashIndex := makeCuckooHashIndex(table.hostPrimaryKeyData, table.devicePrimaryKeyPtr.getPointer())
	doCGoCall(func() C.CGoCallResHandle {
		return C.HashLookup(
			inputVector, (*C.RecordID)(bc.foreignTableRecordIDsD[joinTableID].getPointer()),
			(*C.uint32_t)(bc.indexVectorD.getPointer()), (C.int)(bc.size), (*C.uint32_t)(bc.baseCountD.getPointer()),
			(C.uint32_t)(bc.startRow), hashIndex, stream, C.int(device))
	})
}

// processExpression does AST tree dfs traversal and apply root action on the root level,
// rootAction includes filterAction, writeToDimensionVectorAction and makeWriteToMeasureVectorAction
func (bc *oopkBatchContext) processExpression(exp, parentExp expr.Expr, tableScanners []*TableScanner, foreignTables []*foreignTable,
	stream unsafe.Pointer, device int, action rootAction) C.InputVector {
	switch e := exp.(type) {
	case *expr.ParenExpr:
		return bc.processExpression(e.Expr, e, tableScanners, foreignTables, stream, device, action)
	case *expr.VarRef:
		columnIndex := tableScanners[e.TableID].ColumnsByIDs[e.ColumnID]
		var inputVector C.InputVector
		// main table
		if e.TableID == 0 {
			column := bc.columns[columnIndex]
			inputVector = makeVectorPartySliceInput(column)
		} else {
			var timezoneLookup unsafe.Pointer
			var timezoneLookupDSize int
			switch pe := parentExp.(type) {
			case *expr.BinaryExpr:
				if pe.Op == expr.CONVERT_TZ {
					timezoneLookup = bc.timezoneLookupD.getPointer()
					timezoneLookupDSize = bc.timezoneLookupDSize
				}
			default:
			}
			inputVector = makeForeignColumnInput(columnIndex, bc.foreignTableRecordIDsD[e.TableID-1].getPointer(), *foreignTables[e.TableID-1], timezoneLookup, timezoneLookupDSize)
		}

		if action != nil {
			action(C.Noop, stream, device, []C.InputVector{inputVector}, e)
			return C.InputVector{}
		}
		return inputVector
	case *expr.NumberLiteral, *expr.GeopointLiteral, *expr.UUIDLiteral:
		var inputVector C.InputVector
		inputVector = makeConstantInput(e, true)
		if action != nil {
			action(C.Noop, stream, device, []C.InputVector{inputVector}, e)
			return C.InputVector{}
		}
		return inputVector
	case *expr.UnaryExpr:
		inputVector := bc.processExpression(e.Expr, e, tableScanners, foreignTables, stream, device, nil)
		functorType, exist := UnaryExprTypeToCFunctorType[e.Op]
		if !exist {
			functorType = C.Noop
		}

		if action != nil {
			action(functorType, stream, device, []C.InputVector{inputVector}, e)
			return C.InputVector{}
		}

		dataType := getOutputDataType(e.Type(), 4)
		values, nulls := bc.allocateStackFrame(dataType)
		var outputVector = makeScratchSpaceOutput(values.getPointer(), nulls.getPointer(), dataType)

		doCGoCall(func() C.CGoCallResHandle {
			return C.UnaryTransform(inputVector, outputVector, (*C.uint32_t)(bc.indexVectorD.getPointer()),
				(C.int)(bc.size), (*C.uint32_t)(bc.baseCountD.getPointer()), (C.uint32_t)(bc.startRow), functorType, stream, C.int(device))
		})

		if inputVector.Type == C.ScratchSpaceInput {
			bc.shrinkStackFrame()
		}
		return makeScratchSpaceInput(values.getPointer(), nulls.getPointer(), dataType)
	case *expr.BinaryExpr:
		lhsInputVector := bc.processExpression(e.LHS, e, tableScanners, foreignTables, stream, device, nil)

		rhsInputVector := bc.processExpression(e.RHS, e, tableScanners, foreignTables, stream, device, nil)
		functorType, exist := BinaryExprTypeToCFunctorType[e.Op]
		if !exist {
			return makeConstantInput(0.0, false)
		}

		if action != nil {
			action(functorType, stream, device, []C.InputVector{lhsInputVector, rhsInputVector}, e)
			return C.InputVector{}
		}

		outputDataType := getOutputDataType(e.Type(), 4)
		values, nulls := bc.allocateStackFrame(outputDataType)
		var outputVector = makeScratchSpaceOutput(values.getPointer(), nulls.getPointer(), outputDataType)

		doCGoCall(func() C.CGoCallResHandle {
			return C.BinaryTransform(lhsInputVector, rhsInputVector, outputVector,
				(*C.uint32_t)(bc.indexVectorD.getPointer()), (C.int)(bc.size), (*C.uint32_t)(bc.baseCountD.getPointer()),
				(C.uint32_t)(bc.startRow),
				functorType, stream, C.int(device))
		})

		if rhsInputVector.Type == C.ScratchSpaceInput {
			bc.shrinkStackFrame()
		}

		if lhsInputVector.Type == C.ScratchSpaceInput {
			bc.shrinkStackFrame()
		}
		return makeScratchSpaceInput(values.getPointer(), nulls.getPointer(), outputDataType)
	default:
		return C.InputVector{}
	}
}

func makeGeoShapeBatch(shapesLatLongs devicePointer, numShapes, totalNumPoints int) C.GeoShapeBatch {
	var geoShapes C.GeoShapeBatch
	geoShapes.LatLongs = (*C.uint8_t)(shapesLatLongs.getPointer())
	totalWords := (numShapes + 31) / 32
	geoShapes.TotalNumPoints = (C.int32_t)(totalNumPoints)
	geoShapes.TotalWords = (C.uint8_t)(totalWords)
	return geoShapes
}

func (bc *oopkBatchContext) makeGeoPointInputVector(pointTableID int, pointColumnIndex int, foreignTables []*foreignTable) C.InputVector {
	if pointTableID == 0 {
		return makeVectorPartySliceInput(bc.columns[pointColumnIndex])
	}
	return makeForeignColumnInput(pointColumnIndex,
		bc.foreignTableRecordIDsD[pointTableID-1].getPointer(),
		*foreignTables[pointTableID-1], nil, 0)
}

func (bc *oopkBatchContext) writeGeoShapeDim(geo *geoIntersection,
	outputPredicate devicePointer, dimValueOffset, dimNullOffset int, sizeBeforeGeoFilter, prevResultSize int, stream unsafe.Pointer, device int) {
	if bc.size <= 0 || geo.shapeLatLongs.isNull() {
		return
	}

	// geo dimension always take 1 byte and has type uint8
	// compiler should have checked the number of geo shapes for join is less than 256
	var dimensionOutputVector C.DimensionOutputVector
	dimensionVector := bc.dimensionVectorD[0].getPointer()
	// move dimensionVectorD to the start position of current batch
	// dimension vector start position + prevResultSize * dataBytes
	// null vector start position + prevResultSize
	dimensionOutputVector.DimValues = (*C.uint8_t)(utils.MemAccess(dimensionVector, dimValueOffset+prevResultSize))
	dimensionOutputVector.DimNulls = (*C.uint8_t)(utils.MemAccess(dimensionVector, dimNullOffset+prevResultSize))
	dimensionOutputVector.DataType = C.Uint8

	totalWords := (geo.numShapes + 31) / 32
	doCGoCall(func() C.CGoCallResHandle {
		return C.WriteGeoShapeDim((C.int)(totalWords), dimensionOutputVector, (C.int)(sizeBeforeGeoFilter),
			(*C.uint32_t)(outputPredicate.getPointer()), stream, (C.int)(device))
	})
}

func (bc *oopkBatchContext) geoIntersect(geo *geoIntersection, pointColumnIndex int,
	foreignTables []*foreignTable,
	outputPredicte devicePointer, stream unsafe.Pointer, device int) {
	// If current batch size is already 0, short circuit to avoid issuing a noop cuda call.
	if bc.size <= 0 || geo.shapeLatLongs.isNull() {
		return
	}

	numForeignTables := len(bc.foreignTableRecordIDsD)
	foreignTableRecordIDs := unsafe.Pointer(nil)
	if numForeignTables > 0 {
		foreignTableRecordIDs = unsafe.Pointer(&bc.foreignTableRecordIDsD[0].pointer)
	}
	geoShapes := makeGeoShapeBatch(geo.shapeLatLongs, geo.numShapes, geo.totalNumPoints)
	points := bc.makeGeoPointInputVector(geo.pointTableID, pointColumnIndex, foreignTables)
	bc.size = int(doCGoCall(func() C.CGoCallResHandle {
		return C.GeoBatchIntersects(
			geoShapes, points, (*C.uint32_t)(bc.indexVectorD.getPointer()),
			(C.int)(bc.size), (C.uint32_t)(bc.startRow), (**C.RecordID)(foreignTableRecordIDs),
			(C.int)(numForeignTables), (*C.uint32_t)(outputPredicte.getPointer()),
			(C.bool)(geo.inOrOut), stream, (C.int)(device))
	}))
}

func (bc *oopkBatchContext) hll(numDims common.DimCountsPerDimWidth, isLastBatch bool, stream unsafe.Pointer, device int) (
	hllVector, dimRegCount devicePointer, hllVectorSize int64) {
	prevDimOut := makeDimensionVector(bc.dimensionVectorD[0].getPointer(), bc.hashVectorD[0].getPointer(),
		bc.dimIndexVectorD[0].getPointer(), numDims, bc.resultCapacity)
	curDimOut := makeDimensionVector(bc.dimensionVectorD[1].getPointer(), bc.hashVectorD[1].getPointer(),
		bc.dimIndexVectorD[1].getPointer(), numDims, bc.resultCapacity)
	prevValuesOut, curValuesOut := (*C.uint32_t)(bc.measureVectorD[0].getPointer()), (*C.uint32_t)(bc.measureVectorD[1].getPointer())
	bc.resultSize = int(doCGoCall(func() C.CGoCallResHandle {
		return C.HyperLogLog(prevDimOut, curDimOut,
			prevValuesOut, curValuesOut,
			(C.int)(bc.resultSize), (C.int)(bc.size), (C.bool)(isLastBatch),
			(**C.uint8_t)(unsafe.Pointer(&hllVector.pointer)), (*C.size_t)(unsafe.Pointer(&hllVectorSize)),
			(**C.uint16_t)(unsafe.Pointer(&dimRegCount.pointer)), stream, (C.int)(device))
	}))
	// TODO: we also need a way to report this allocation in C++ code. Maybe can be done via calling a golang function from c++
	hllVector.device = device
	hllVector.allocated = true
	dimRegCount.device = device
	dimRegCount.allocated = true
	return
}

func (bc *oopkBatchContext) sortByKey(numDims common.DimCountsPerDimWidth, stream unsafe.Pointer, device int) {
	keys := makeDimensionVector(bc.dimensionVectorD[0].getPointer(), bc.hashVectorD[0].getPointer(),
		bc.dimIndexVectorD[0].getPointer(), numDims, bc.resultCapacity)
	doCGoCall(func() C.CGoCallResHandle {
		// sort the previous result with current batch together
		return C.Sort(keys, (C.int)(bc.resultSize+bc.size), stream, C.int(device))
	})
}

func (bc *oopkBatchContext) reduceByKey(numDims common.DimCountsPerDimWidth, valueWidth int, aggFunc C.enum_AggregateFunction, stream unsafe.Pointer,
	device int) {
	inputKeys := makeDimensionVector(
		bc.dimensionVectorD[0].getPointer(), bc.hashVectorD[0].getPointer(), bc.dimIndexVectorD[0].getPointer(), numDims, bc.resultCapacity)
	outputKeys := makeDimensionVector(
		bc.dimensionVectorD[1].getPointer(), bc.hashVectorD[1].getPointer(), bc.dimIndexVectorD[1].getPointer(), numDims, bc.resultCapacity)
	inputValues, outputValues := (*C.uint8_t)(bc.measureVectorD[0].getPointer()), (*C.uint8_t)(bc.measureVectorD[1].getPointer())
	bc.resultSize = int(doCGoCall(func() C.CGoCallResHandle {
		return C.Reduce(inputKeys, inputValues, outputKeys, outputValues, (C.int)(valueWidth), (C.int)(bc.resultSize+bc.size), aggFunc,
			stream, C.int(device))
	}))
}

func (bc *oopkBatchContext) hashReduce(numDims common.DimCountsPerDimWidth, valueWidth int, aggFunc C.enum_AggregateFunction, stream unsafe.Pointer,
	device int) {
	inputKeys := makeDimensionVector(
		bc.dimensionVectorD[0].getPointer(), bc.hashVectorD[0].getPointer(), bc.dimIndexVectorD[0].getPointer(), numDims, bc.resultCapacity)
	outputKeys := makeDimensionVector(
		bc.dimensionVectorD[1].getPointer(), bc.hashVectorD[1].getPointer(), bc.dimIndexVectorD[1].getPointer(), numDims, bc.resultCapacity)
	inputValues, outputValues := (*C.uint8_t)(bc.measureVectorD[0].getPointer()), (*C.uint8_t)(bc.measureVectorD[1].getPointer())
	bc.resultSize = int(doCGoCall(func() C.CGoCallResHandle {
		return C.HashReduce(inputKeys, inputValues, outputKeys, outputValues, (C.int)(valueWidth), (C.int)(bc.resultSize+bc.size), aggFunc,
			stream, C.int(device))
	}))
}

func (bc *oopkBatchContext) expand(numDims common.DimCountsPerDimWidth, stream unsafe.Pointer, device int) {
	inputKeys := makeDimensionVector(
		bc.dimensionVectorD[0].getPointer(), bc.hashVectorD[0].getPointer(), bc.dimIndexVectorD[0].getPointer(), numDims, bc.resultCapacity)
	outputKeys := makeDimensionVector(
		bc.dimensionVectorD[1].getPointer(), bc.hashVectorD[1].getPointer(), bc.dimIndexVectorD[1].getPointer(), numDims, bc.resultCapacity)

	bc.resultSize = int(doCGoCall(func() C.CGoCallResHandle {
		return C.Expand(inputKeys, outputKeys, (*C.uint32_t)(bc.baseCountD.getPointer()), (*C.uint32_t)(bc.indexVectorD.getPointer()),
			C.int(bc.size), 0, stream, C.int(device))
	}))
	bc.dimensionVectorD[0], bc.dimensionVectorD[1] = bc.dimensionVectorD[1], bc.dimensionVectorD[0]
}

func (bc *oopkBatchContext) getDataTypeLength(dataType C.enum_DataType) int {
	switch dataType {
	case C.Int64, C.Uint64, C.Float64:
		return 8
	case C.GeoPoint:
		return 8
	case C.UUID:
		return 16
	default:
		return 4
	}
}

// allocate new stack frame and append to the end of the stack
func (bc *oopkBatchContext) allocateStackFrame(dataType C.enum_DataType) (values, nulls devicePointer) {
	dataTypeLen := bc.getDataTypeLength(dataType)
	// width bytes * bc.size (value buffer) + 1 byte * bc.size (null buffer)
	valuesPointer := deviceAllocate((dataTypeLen+1)*bc.size, bc.device)
	nullsPointer := valuesPointer.offset(dataTypeLen * bc.size)
	// append output buffer to the end
	bc.exprStackD = append(bc.exprStackD, [2]devicePointer{valuesPointer, nullsPointer})
	return valuesPointer, nullsPointer
}

// shrink stack by one, but keep top element as is
func (bc *oopkBatchContext) shrinkStackFrame() {
	var stackFrame [2]devicePointer
	// swap last two elements
	bc.exprStackD[len(bc.exprStackD)-1], bc.exprStackD[len(bc.exprStackD)-2] = bc.exprStackD[len(bc.exprStackD)-2], bc.exprStackD[len(bc.exprStackD)-1]
	// pop last element
	stackFrame, bc.exprStackD = bc.exprStackD[len(bc.exprStackD)-1], bc.exprStackD[:len(bc.exprStackD)-1]
	deviceFreeAndSetNil(&stackFrame[0])
}

func (qc *AQLQueryContext) createCutoffTimeFilter(cutoff uint32) expr.Expr {
	column := &expr.VarRef{
		Val:      qc.Query.TimeFilter.Column,
		ExprType: expr.Unsigned,
		TableID:  0,
		// time column is always 0
		ColumnID: 0,
	}

	return &expr.BinaryExpr{
		ExprType: expr.Boolean,
		Op:       expr.GTE,
		LHS:      column,
		RHS: &expr.NumberLiteral{
			Int:      int(cutoff),
			Expr:     strconv.FormatInt(int64(cutoff), 10),
			ExprType: expr.Unsigned,
		},
	}
}

// doCGoCall does the cgo call by converting CGoCallResHandle to C.int and *C.char and calls doCGoCall.
// The reason to have this wrapper is because CGo types are bound to package name, thereby even C.int are different types
// under different packages.
func doCGoCall(f func() C.CGoCallResHandle) uintptr {
	return cgoutils.DoCGoCall(func() (uintptr, unsafe.Pointer) {
		ret := f()
		return uintptr(ret.res), unsafe.Pointer(ret.pStrErr)
	})
}

// bootstrapDevice is the go wrapper of BootstrapDevice. It will panic and crash the server if any exceptions are thrown
// in this function.
func bootstrapDevice() {
	doCGoCall(func() C.CGoCallResHandle {
		return C.BootstrapDevice()
	})
}

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
	"bytes"
	"github.com/uber/aresdb/utils"
	"strings"

	"github.com/pkg/errors"
	memCom "github.com/uber/aresdb/memstore/common"
	"io"
	"math"
	"sort"
	"unsafe"
)

const (
	// OldHLLDataHeader is the old magic header for migration
	OldHLLDataHeader uint32 = 0xACED0101
	// HLLDataHeader is the magic header written into serialized format of hyperloglog query result.
	HLLDataHeader uint32 = 0xACED0102
	// EnumDelimiter is the delimiter to delimit enum cases.
	EnumDelimiter = "\u0000\n"
	// DenseDataLength is the length of hll dense data in bytes.
	DenseDataLength = 1 << 14 // 16kb
	// DenseThreshold is the thresold to convert sparse value to dense value.
	DenseThreshold = DenseDataLength / 4
)

// HLLData stores fields for serialize and deserialize an hyperloglog query result when client sets Content-Accept
// header to be application/hll.
// The serialized buffer of a hll data is in following format:
//	 [uint32] magic_number [uint32] padding
//
//	-----------query result 0-------------------
//	 <header>
//	 [uint32] query result 0 size [uint8] error or result [3 bytes padding]
//	 [uint8] num_enum_columns [uint8] bytes per dim ... [padding for 8 bytes]
//	 [uint32] result_size [uint32] raw_dim_values_vector_length
//	 [uint8] dim_index_0... [uint8] dim_index_n [padding for 8 bytes]
//	 [uint32] data_type_0...[uint32] data_type_n [padding for 8 bytes]
//
//	 <enum cases 0>
//	 [uint32_t] number of bytes of enum cases [uint16] column_index [2 bytes: padding]
//	 <enum values 0> delimited by "\u0000\n" [padding for 8 bytes]
//	 <end of header>
//	 <raw dim values vector>
//	 ...
//	 [padding for 8 byte alignment]
//
//	 <raw hll dense vector>
//	 ...
//	------------error 1----------
//	 [uint32] query result 1 size  [uint8] error or result [3 bytes padding]
//	...
type HLLData struct {
	NumDimsPerDimWidth             DimCountsPerDimWidth
	ResultSize                     uint32
	PaddedRawDimValuesVectorLength uint32
	PaddedHLLVectorLength          int64

	DimIndexes []int
	DataTypes  []memCom.DataType
	// map from column id => enum cases. It will
	// only include columns used in dimensions.
	EnumDicts map[int][]string
}

// CalculateSizes returns the header size and total size of used by this hll data.
func (data *HLLData) CalculateSizes() (uint32, int64) {
	// num enum columns (1 byte)
	var headerSize = 1
	// Dims per width (1 byte * numDims)
	headerSize += len(data.NumDimsPerDimWidth)
	// padding for 8 bytes
	headerSize = utils.AlignOffset(headerSize, 8)
	// result size (4 bytes) + raw_dim_values_vector_length (4 bytes)
	headerSize += 8

	// Dim indexes.
	headerSize += (len(data.DimIndexes) + 7) / 8 * 8

	// Data types.
	headerSize += (len(data.DataTypes)*4 + 7) / 8 * 8

	// Enum cases.
	for _, enumCases := range data.EnumDicts {
		// number of bytes of enum cases + column index + padding = 8 bytes.
		headerSize += int(8 + CalculateEnumCasesBytes(enumCases))
	}

	totalSize := int64(headerSize)

	// Dim values.
	totalSize += int64(data.PaddedRawDimValuesVectorLength)

	// Counts.
	totalSize += int64(2*data.ResultSize+7) / 8 * 8

	// HLL dense vector.
	totalSize += data.PaddedHLLVectorLength

	return uint32(headerSize), totalSize
}

// CalculateEnumCasesBytes calculates how many bytes the enum case values will occupy including 8 bytes alignment.
func CalculateEnumCasesBytes(enumCases []string) uint32 {
	var size uint32

	for _, enumCase := range enumCases {
		size += uint32(len(enumCase))
	}

	// enum cases delimiters.
	size += uint32(len(enumCases)) * 2

	// align by 8 bytes.
	return (size + 7) / 8 * 8
}

// HLLRegister is the register used in the sparse representation.
type HLLRegister struct {
	Index uint16 `json:"index"`
	Rho   byte   `json:"rho"`
}

// HLL stores only the dense data for now.
type HLL struct {
	SparseData       []HLLRegister // Unsorted registers.
	DenseData        []byte        // Rho by register index.
	NonZeroRegisters uint16
}

// Merge merges (using max(rho)) the other HLL (sparse or dense) into this one (will be converted to dense).
func (hll *HLL) Merge(other HLL) {
	hll.ConvertToDense()
	for _, register := range other.SparseData {
		oldRho := hll.DenseData[register.Index]
		if oldRho == 0 {
			hll.NonZeroRegisters++
		}
		if oldRho < register.Rho {
			hll.DenseData[register.Index] = register.Rho
		}
	}
	for index, rho := range other.DenseData {
		oldRho := hll.DenseData[index]
		if oldRho == 0 && rho != 0 {
			hll.NonZeroRegisters++
		}
		if oldRho < rho {
			hll.DenseData[index] = rho
		}
	}
}

// ConvertToDense converts the HLL to dense format.
func (hll *HLL) ConvertToDense() {
	if len(hll.DenseData) != 0 {
		return
	}

	hll.DenseData = make([]byte, 1<<hllP)
	for _, register := range hll.SparseData {
		hll.DenseData[register.Index] = register.Rho
	}
	hll.SparseData = nil
}

// ConvertToSparse try converting the hll to sparse format if it turns out to be cheaper.
func (hll *HLL) ConvertToSparse() bool {
	if hll.NonZeroRegisters*4 >= 1<<hllP {
		return false
	}
	if hll.SparseData != nil {
		return true
	}
	hll.SparseData = make([]HLLRegister, 0, hll.NonZeroRegisters)
	for index, rho := range hll.DenseData {
		if rho != 0 {
			hll.SparseData = append(hll.SparseData, HLLRegister{uint16(index), rho})
		}
	}
	hll.DenseData = nil
	return true
}

// Set sets rho for the specified register index. Caller must ensure that each register is set no more than once.
func (hll *HLL) Set(index uint16, rho byte) {
	hll.NonZeroRegisters++

	if len(hll.DenseData) != 0 {
		hll.DenseData[index] = rho
		return
	}

	hll.SparseData = append(hll.SparseData, HLLRegister{index, rho})

	if hll.NonZeroRegisters*4 >= 1<<hllP {
		hll.ConvertToDense()
	}
}

func parseOldTimeseriesHLLResult(buffer []byte) (AQLQueryResult, error) {
	// empty result buffer
	if len(buffer) == 0 {
		return AQLQueryResult{}, nil
	}

	reader := utils.NewStreamDataReader(bytes.NewBuffer(buffer))

	numFourBytesDims, err := reader.ReadUint8()
	if err != nil {
		return nil, err
	}

	numTwoBytesDims, err := reader.ReadUint8()
	if err != nil {
		return nil, err
	}

	numOneBytesDims, err := reader.ReadUint8()
	if err != nil {
		return nil, err
	}

	numEnumColumns, err := reader.ReadUint8()
	if err != nil {
		return nil, err
	}

	totalDims := int(numFourBytesDims + numTwoBytesDims + numOneBytesDims)

	numDimsPerDimWidth := DimCountsPerDimWidth{0, 0, numFourBytesDims, numTwoBytesDims, numOneBytesDims}

	resultSize, err := reader.ReadUint32()
	if err != nil {
		return nil, err
	}

	paddedRawDimValuesVectorLength, err := reader.ReadUint32()
	if err != nil {
		return nil, err
	}

	if err := reader.SkipBytes(4); err != nil {
		return nil, err
	}

	dimIndexes := make([]uint8, totalDims)

	for i := range dimIndexes {
		dimIndexes[i], err = reader.ReadUint8()
		if err != nil {
			return nil, err
		}
	}

	if err = reader.ReadPadding(int(totalDims), 8); err != nil {
		return nil, err
	}

	dataTypes := make([]memCom.DataType, totalDims)

	for i := range dataTypes {
		rawDataType, err := reader.ReadUint32()
		if err != nil {
			return nil, err
		}

		dataType, err := memCom.NewDataType(rawDataType)
		if err != nil {
			return nil, err
		}

		dataTypes[i] = dataType
	}

	if err = reader.ReadPadding(int(totalDims)*4, 8); err != nil {
		return nil, err
	}

	enumDicts := make(map[int][]string)
	var i uint8
	for ; i < numEnumColumns; i++ {
		enumCasesBytes, err := reader.ReadUint32()
		if err != nil {
			return nil, err
		}

		columnID, err := reader.ReadUint16()
		if err != nil {
			return nil, err
		}
		reader.SkipBytes(2)
		rawEnumCases := make([]byte, enumCasesBytes)
		if err = reader.Read(rawEnumCases); err != nil {
			return nil, err
		}

		enumCases := strings.Split(string(rawEnumCases), EnumDelimiter)

		// remove last empty element.
		enumCases = enumCases[:len(enumCases)-1]
		enumDicts[int(columnID)] = enumCases
	}

	headerSize := reader.GetBytesRead()

	result := make(AQLQueryResult)

	paddedCountLength := uint32(2*resultSize+7) / 8 * 8

	dimValuesVector := unsafe.Pointer(&buffer[headerSize])

	countVector := unsafe.Pointer(&buffer[headerSize+paddedRawDimValuesVectorLength])

	hllVector := unsafe.Pointer(&buffer[headerSize+paddedRawDimValuesVectorLength+paddedCountLength])

	dimOffsets := make([][2]int, totalDims)
	dimValues := make([]*string, totalDims)

	for i := 0; i < totalDims; i++ {
		dimIndex := int(dimIndexes[i])
		valueOffset, nullOffset := GetDimensionStartOffsets(numDimsPerDimWidth, dimIndex, int(resultSize))
		dimOffsets[i] = [2]int{valueOffset, nullOffset}
	}

	var currentOffset int64

	for i := 0; i < int(resultSize); i++ {
		for dimIndex := 0; dimIndex < totalDims; dimIndex++ {
			offsets := dimOffsets[dimIndex]
			valueOffset, nullOffset := offsets[0], offsets[1]
			valuePtr, nullPtr := memAccess(dimValuesVector, valueOffset), memAccess(dimValuesVector, nullOffset)
			dimValues[dimIndex] = ReadDimension(valuePtr, nullPtr, i, dataTypes[dimIndex], enumDicts[dimIndex], nil, nil)
		}

		count := *(*uint16)(memAccess(countVector, int(2*i)))
		hll := readHLL(hllVector, count, &currentOffset)
		result.SetHLL(dimValues, hll)
	}

	return result, nil
}

func parseTimeseriesHLLResult(buffer []byte) (AQLQueryResult, error) {
	// empty result buffer
	if len(buffer) == 0 {
		return AQLQueryResult{}, nil
	}

	reader := utils.NewStreamDataReader(bytes.NewBuffer(buffer))
	numEnumColumns, err := reader.ReadUint8()
	if err != nil {
		return nil, err
	}

	var numDimsPerDimWidth DimCountsPerDimWidth
	err = reader.Read([]byte(numDimsPerDimWidth[:]))
	if err != nil {
		return AQLQueryResult{}, nil
	}

	totalDims := 0
	for _, dimCount := range numDimsPerDimWidth {
		totalDims += int(dimCount)
	}

	err = reader.ReadPadding(int(reader.GetBytesRead()), 8)
	if err != nil {
		return nil, err
	}

	resultSize, err := reader.ReadUint32()
	if err != nil {
		return nil, err
	}

	paddedRawDimValuesVectorLength, err := reader.ReadUint32()
	if err != nil {
		return nil, err
	}

	dimIndexes := make([]uint8, totalDims)
	for i := range dimIndexes {
		dimIndexes[i], err = reader.ReadUint8()
		if err != nil {
			return nil, err
		}
	}

	if err = reader.ReadPadding(int(totalDims), 8); err != nil {
		return nil, err
	}

	dataTypes := make([]memCom.DataType, totalDims)

	for i := range dataTypes {
		rawDataType, err := reader.ReadUint32()
		if err != nil {
			return nil, err
		}

		dataType, err := memCom.NewDataType(rawDataType)
		if err != nil {
			return nil, err
		}

		dataTypes[i] = dataType
	}

	if err = reader.ReadPadding(int(totalDims)*4, 8); err != nil {
		return nil, err
	}

	enumDicts := make(map[int][]string)
	var i uint8
	for ; i < numEnumColumns; i++ {
		enumCasesBytes, err := reader.ReadUint32()
		if err != nil {
			return nil, err
		}

		columnID, err := reader.ReadUint16()
		if err != nil {
			return nil, err
		}
		reader.SkipBytes(2)
		rawEnumCases := make([]byte, enumCasesBytes)
		if err = reader.Read(rawEnumCases); err != nil {
			return nil, err
		}

		enumCases := strings.Split(string(rawEnumCases), EnumDelimiter)

		// remove last empty element.
		enumCases = enumCases[:len(enumCases)-1]
		enumDicts[int(columnID)] = enumCases
	}

	headerSize := reader.GetBytesRead()

	result := make(AQLQueryResult)

	paddedCountLength := uint32(2*resultSize+7) / 8 * 8

	dimValuesVector := unsafe.Pointer(&buffer[headerSize])

	countVector := unsafe.Pointer(&buffer[headerSize+paddedRawDimValuesVectorLength])

	hllVector := unsafe.Pointer(&buffer[headerSize+paddedRawDimValuesVectorLength+paddedCountLength])

	dimOffsets := make([][2]int, totalDims)
	dimValues := make([]*string, totalDims)

	for i := 0; i < totalDims; i++ {
		dimIndex := int(dimIndexes[i])
		valueOffset, nullOffset := GetDimensionStartOffsets(numDimsPerDimWidth, dimIndex, int(resultSize))
		dimOffsets[i] = [2]int{valueOffset, nullOffset}
	}

	var currentOffset int64

	for i := 0; i < int(resultSize); i++ {
		for dimIndex := 0; dimIndex < totalDims; dimIndex++ {
			offsets := dimOffsets[dimIndex]
			valueOffset, nullOffset := offsets[0], offsets[1]
			valuePtr, nullPtr := memAccess(dimValuesVector, valueOffset), memAccess(dimValuesVector, nullOffset)
			dimValues[dimIndex] = ReadDimension(valuePtr, nullPtr, i, dataTypes[dimIndex], enumDicts[dimIndex], nil, nil)
		}

		count := *(*uint16)(memAccess(countVector, int(2*i)))
		hll := readHLL(hllVector, count, &currentOffset)
		result.SetHLL(dimValues, hll)
	}

	return result, nil
}

// ComputeHLLResult computes hll result
func ComputeHLLResult(result AQLQueryResult) AQLQueryResult {
	return computeHLLResultRecursive(result).(AQLQueryResult)
}

// computeHLLResultRecursive computes hll value
func computeHLLResultRecursive(result interface{}) interface{} {
	switch r := result.(type) {
	case AQLQueryResult:
		for k, v := range r {
			r[k] = computeHLLResultRecursive(v)
		}
		return r
	case map[string]interface{}:
		for k, v := range r {
			r[k] = computeHLLResultRecursive(v)
		}
		return r
	case HLL:
		return r.Compute()
	default:
		// return original for all other types
		return r
	}
}

// NewTimeSeriesHLLResult creates a new NewTimeSeriesHLLResult and deserialize the buffer into the result.
func NewTimeSeriesHLLResult(buffer []byte, magicHeader uint32) (AQLQueryResult, error) {
	switch magicHeader {
	case OldHLLDataHeader:
		return parseOldTimeseriesHLLResult(buffer)
	case HLLDataHeader:
		return parseTimeseriesHLLResult(buffer)
	default:
		// should not happen
		return nil, utils.StackError(nil, "magic header version unsupported: %d", magicHeader)
	}
}

// memAccess access memory location with starting pointer and an offset.
func memAccess(p unsafe.Pointer, offset int) unsafe.Pointer {
	return unsafe.Pointer(uintptr(p) + uintptr(offset))
}

// readHLL reads the HLL struct from the raw buffer and returns next offset
func readHLL(hllVector unsafe.Pointer, count uint16, currentOffset *int64) HLL {
	var sparseData []HLLRegister
	var nonZeroRegisters uint16
	var denseData []byte
	if count < DenseThreshold {
		var i uint16
		sparseData = make([]HLLRegister, 0, count)
		for ; i < count; i++ {
			data := *(*uint32)(memAccess(hllVector, int(*currentOffset)))
			index := uint16(data) // Big-endian from UNHEX...
			rho := byte((data >> 16) & 0xFF)
			sparseData = append(sparseData, HLLRegister{
				Index: index,
				Rho:   rho,
			})
			*currentOffset += 4
		}
		nonZeroRegisters = count
	} else {
		denseData = (*(*[DenseDataLength]byte)((memAccess(hllVector, int(*currentOffset)))))[:]
		*currentOffset += DenseDataLength
		for _, b := range denseData {
			if b != 0 {
				nonZeroRegisters++
			}
		}
	}

	return HLL{
		DenseData:        denseData,
		SparseData:       sparseData,
		NonZeroRegisters: nonZeroRegisters,
	}
}

// ParseHLLQueryResults will parse the response body into a slice of query results and a slice of errors.
func ParseHLLQueryResults(data []byte) (queryResults []AQLQueryResult, queryErrors []error, err error) {
	reader := utils.NewStreamDataReader(bytes.NewBuffer(data))

	var magicHeader uint32
	magicHeader, err = reader.ReadUint32()
	if err != nil {
		return
	}

	if magicHeader != OldHLLDataHeader && magicHeader != HLLDataHeader {
		err = utils.StackError(nil, "header %x does not match HLLDataHeader %x or %x",
			magicHeader, OldHLLDataHeader, HLLDataHeader)
		return
	}

	reader.SkipBytes(4)

	var size uint32
	var isErr uint8

	for size, err = reader.ReadUint32(); err == nil; size, err = reader.ReadUint32() {
		if isErr, err = reader.ReadUint8(); err != nil {
			return
		}

		reader.SkipBytes(3)

		bs := make([]byte, size)
		err = reader.Read(bs)
		if err != nil {
			break
		}

		if isErr != 0 {
			queryErrors = append(queryErrors, errors.New(string(bs)))
			queryResults = append(queryResults, nil)
		} else {
			var res AQLQueryResult
			if res, err = NewTimeSeriesHLLResult(bs, magicHeader); err != nil {
				return
			}
			queryResults = append(queryResults, res)
			queryErrors = append(queryErrors, nil)
		}
	}

	if err == io.EOF {
		err = nil
	}
	return
}

type hllBiasByDistance struct {
	distance, bias float64
}

func getEstimateBias(estimate float64) float64 {
	i := sort.Search(len(hllRawEstimates), func(i int) bool { return estimate < hllRawEstimates[i] })

	// Find nearest k neighbors.
	k := 6
	startIdx := i - 1 - k
	endIdx := i + k
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx > len(hllRawEstimates) {
		endIdx = len(hllRawEstimates)
	}
	biases := make(hllBiasesByDistances, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		biases[i-startIdx].distance = (hllRawEstimates[i] - estimate) * (hllRawEstimates[i] - estimate)
		biases[i-startIdx].bias = hllBiases[i]
	}
	sort.Sort(biases)

	biasSum := 0.0
	for i := 0; i < k; i++ {
		biasSum += biases[i].bias
	}

	return biasSum / float64(k)
}

// Decode decodes the HLL from cache cache.
// Interprets as dense or sparse format based on len(data).
func (hll *HLL) Decode(data []byte) {
	if len(data) == 1<<hllP {
		hll.DenseData = data
		hll.SparseData = nil
		hll.NonZeroRegisters = 0
		for _, rho := range data {
			if rho != 0 {
				hll.NonZeroRegisters++
			}
		}
	} else {
		hll.DenseData = nil
		hll.SparseData = make([]HLLRegister, len(data)/3)
		hll.NonZeroRegisters = uint16(len(data) / 3)
		for i := 0; i < len(data)/3; i++ {
			var register HLLRegister
			register.Index = uint16(data[i*3]) | (uint16(data[i*3+1]) << 8)
			register.Rho = data[i*3+2]
			hll.SparseData[i] = register
		}
	}
}

// Encode encodes the HLL for cache storage.
// Dense format will have a length of 1<<hllP.
// Sparse format will have a smaller length
func (hll *HLL) Encode() []byte {
	if len(hll.DenseData) != 0 {
		return hll.DenseData
	}
	return hll.encodeSparse(false)
}

// EncodeBinary converts HLL to binary format
// aligns to 4 bytes for sparse hll
// used to build response for application/hll queries from HLL struct
func (hll *HLL) EncodeBinary() []byte {
	if len(hll.DenseData) != 0 {
		return hll.DenseData
	}
	return hll.encodeSparse(true)
}

func (hll *HLL) encodeSparse(padding bool) []byte {
	var (
		recordValueBytes = 3
		paddingBytes     = 0
	)
	if padding {
		paddingBytes = 1
	}
	data := make([]byte, (recordValueBytes+paddingBytes)*len(hll.SparseData))
	for i, register := range hll.SparseData {
		if padding {
			data[i*recordValueBytes+paddingBytes] = byte(0x00)
		}
		data[i*(recordValueBytes+paddingBytes)+paddingBytes] = byte(register.Index & 0xff)
		data[i*(recordValueBytes+paddingBytes)+paddingBytes+1] = byte(register.Index >> 8)
		data[i*(recordValueBytes+paddingBytes)+paddingBytes+2] = register.Rho
	}
	return data
}

// Compute computes the result of the HLL.
func (hll *HLL) Compute() float64 {
	nonZeroRegisters := float64(hll.NonZeroRegisters)
	m := float64(uint64(1) << hllP)

	// Sum of reciproclas of rhos
	var sumOfReciprocals float64
	for _, register := range hll.SparseData {
		sumOfReciprocals += 1.0 / float64(uint64(1)<<register.Rho)
	}
	if len(hll.DenseData) == 0 {
		// Add missing rho reciprocals for sparse form.
		sumOfReciprocals += m - nonZeroRegisters
	}
	for _, rho := range hll.DenseData {
		sumOfReciprocals += 1.0 / float64(uint64(1)<<rho)
	}

	// Initial estimation.
	alpha := 0.7213 / (1 + 1.079/m)
	estimate := alpha * m * m / sumOfReciprocals

	// Bias correction.
	if estimate <= 5.0*m {
		estimate -= getEstimateBias(estimate)
	}

	estimateH := estimate

	if nonZeroRegisters < m {
		// Linear counting
		estimateH = m * math.Log(m/(m-nonZeroRegisters))
	}

	if estimateH <= hllThreshold {
		estimate = estimateH
	}

	// Round
	return float64(uint64(estimate))
}

type hllBiasesByDistances []hllBiasByDistance

func (b hllBiasesByDistances) Len() int      { return len(b) }
func (b hllBiasesByDistances) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b hllBiasesByDistances) Less(i, j int) bool {
	return b[i].distance < b[j].distance
}

// threshold and bias data taken from google's bias correction data set:
// https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen#
var hllP byte = 14

var hllThreshold = 15500.0

// precision 14
var hllRawEstimates = []float64{
	11817.475, 12015.0046, 12215.3792, 12417.7504, 12623.1814, 12830.0086, 13040.0072, 13252.503, 13466.178,
	13683.2738, 13902.0344, 14123.9798, 14347.394, 14573.7784, 14802.6894, 15033.6824, 15266.9134,
	15502.8624, 15741.4944, 15980.7956, 16223.8916, 16468.6316, 16715.733, 16965.5726, 17217.204,
	17470.666, 17727.8516, 17986.7886, 18247.6902, 18510.9632, 18775.304, 19044.7486, 19314.4408,
	19587.202, 19862.2576, 20135.924, 20417.0324, 20697.9788, 20979.6112, 21265.0274, 21550.723,
	21841.6906, 22132.162, 22428.1406, 22722.127, 23020.5606, 23319.7394, 23620.4014, 23925.2728,
	24226.9224, 24535.581, 24845.505, 25155.9618, 25470.3828, 25785.9702, 26103.7764, 26420.4132,
	26742.0186, 27062.8852, 27388.415, 27714.6024, 28042.296, 28365.4494, 28701.1526, 29031.8008,
	29364.2156, 29704.497, 30037.1458, 30380.111, 30723.8168, 31059.5114, 31404.9498, 31751.6752,
	32095.2686, 32444.7792, 32794.767, 33145.204, 33498.4226, 33847.6502, 34209.006, 34560.849,
	34919.4838, 35274.9778, 35635.1322, 35996.3266, 36359.1394, 36722.8266, 37082.8516, 37447.7354,
	37815.9606, 38191.0692, 38559.4106, 38924.8112, 39294.6726, 39663.973, 40042.261, 40416.2036,
	40779.2036, 41161.6436, 41540.9014, 41921.1998, 42294.7698, 42678.5264, 43061.3464, 43432.375,
	43818.432, 44198.6598, 44583.0138, 44970.4794, 45353.924, 45729.858, 46118.2224, 46511.5724,
	46900.7386, 47280.6964, 47668.1472, 48055.6796, 48446.9436, 48838.7146, 49217.7296, 49613.7796,
	50010.7508, 50410.0208, 50793.7886, 51190.2456, 51583.1882, 51971.0796, 52376.5338, 52763.319,
	53165.5534, 53556.5594, 53948.2702, 54346.352, 54748.7914, 55138.577, 55543.4824, 55941.1748,
	56333.7746, 56745.1552, 57142.7944, 57545.2236, 57935.9956, 58348.5268, 58737.5474, 59158.5962,
	59542.6896, 59958.8004, 60349.3788, 60755.0212, 61147.6144, 61548.194, 61946.0696, 62348.6042,
	62763.603, 63162.781, 63560.635, 63974.3482, 64366.4908, 64771.5876, 65176.7346, 65597.3916,
	65995.915, 66394.0384, 66822.9396, 67203.6336, 67612.2032, 68019.0078, 68420.0388, 68821.22,
	69235.8388, 69640.0724, 70055.155, 70466.357, 70863.4266, 71276.2482, 71677.0306, 72080.2006,
	72493.0214, 72893.5952, 73314.5856, 73714.9852, 74125.3022, 74521.2122, 74933.6814, 75341.5904,
	75743.0244, 76166.0278, 76572.1322, 76973.1028, 77381.6284, 77800.6092, 78189.328, 78607.0962,
	79012.2508, 79407.8358, 79825.725, 80238.701, 80646.891, 81035.6436, 81460.0448, 81876.3884}

// precision 14
var hllBiases = []float64{
	11816.475, 11605.0046, 11395.3792, 11188.7504, 10984.1814, 10782.0086, 10582.0072, 10384.503, 10189.178,
	9996.2738, 9806.0344, 9617.9798, 9431.394, 9248.7784, 9067.6894, 8889.6824, 8712.9134, 8538.8624,
	8368.4944, 8197.7956, 8031.8916, 7866.6316, 7703.733, 7544.5726, 7386.204, 7230.666, 7077.8516,
	6926.7886, 6778.6902, 6631.9632, 6487.304, 6346.7486, 6206.4408, 6070.202, 5935.2576, 5799.924,
	5671.0324, 5541.9788, 5414.6112, 5290.0274, 5166.723, 5047.6906, 4929.162, 4815.1406, 4699.127,
	4588.5606, 4477.7394, 4369.4014, 4264.2728, 4155.9224, 4055.581, 3955.505, 3856.9618, 3761.3828,
	3666.9702, 3575.7764, 3482.4132, 3395.0186, 3305.8852, 3221.415, 3138.6024, 3056.296, 2970.4494,
	2896.1526, 2816.8008, 2740.2156, 2670.497, 2594.1458, 2527.111, 2460.8168, 2387.5114, 2322.9498,
	2260.6752, 2194.2686, 2133.7792, 2074.767, 2015.204, 1959.4226, 1898.6502, 1850.006, 1792.849,
	1741.4838, 1687.9778, 1638.1322, 1589.3266, 1543.1394, 1496.8266, 1447.8516, 1402.7354, 1361.9606,
	1327.0692, 1285.4106, 1241.8112, 1201.6726, 1161.973, 1130.261, 1094.2036, 1048.2036, 1020.6436,
	990.901400000002, 961.199800000002, 924.769800000002, 899.526400000002, 872.346400000002, 834.375,
	810.432000000001, 780.659800000001, 756.013800000001, 733.479399999997, 707.923999999999, 673.858,
	652.222399999999, 636.572399999997, 615.738599999997, 586.696400000001, 564.147199999999,
	541.679600000003, 523.943599999999, 505.714599999999, 475.729599999999, 461.779600000002,
	449.750800000002, 439.020799999998, 412.7886, 400.245600000002, 383.188199999997, 362.079599999997,
	357.533799999997, 334.319000000003, 327.553399999997, 308.559399999998, 291.270199999999,
	279.351999999999, 271.791400000002, 252.576999999997, 247.482400000001, 236.174800000001,
	218.774599999997, 220.155200000001, 208.794399999999, 201.223599999998, 182.995600000002, 185.5268,
	164.547400000003, 176.5962, 150.689599999998, 157.8004, 138.378799999999, 134.021200000003,
	117.614399999999, 108.194000000003, 97.0696000000025, 89.6042000000016, 95.6030000000028,
	84.7810000000027, 72.635000000002, 77.3482000000004, 59.4907999999996, 55.5875999999989,
	50.7346000000034, 61.3916000000027, 50.9149999999936, 39.0384000000049, 58.9395999999979,
	29.633600000001, 28.2032000000036, 26.0078000000067, 17.0387999999948, 9.22000000000116,
	13.8387999999977, 8.07240000000456, 14.1549999999988, 15.3570000000036, 3.42660000000615,
	6.24820000000182, -2.96940000000177, -8.79940000000352, -5.97860000000219, -14.4048000000039,
	-3.4143999999942, -13.0148000000045, -11.6977999999945, -25.7878000000055, -22.3185999999987,
	-24.409599999999, -31.9756000000052, -18.9722000000038, -22.8678000000073, -30.8972000000067,
	-32.3715999999986, -22.3907999999938, -43.6720000000059, -35.9038, -39.7492000000057,
	-54.1641999999993, -45.2749999999942, -42.2989999999991, -44.1089999999967, -64.3564000000042,
	-49.9551999999967, -42.6116000000038}

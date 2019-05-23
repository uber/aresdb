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
	"bytes"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"strings"
	"time"
	"unsafe"
	"net/http"
)

type boundaryType int

const (
	noBoundary boundaryType = iota
	inclusiveBoundary
	exclusiveBoundary
)

// columnUsage is a bitmap that tracks how a column is used and whether the
// column should be pushed to device memory for different types of batches.
type columnUsage int

const (
	columnUsedByAllBatches columnUsage = 1 << iota
	columnUsedByLiveBatches
	columnUsedByFirstArchiveBatch
	columnUsedByLastArchiveBatch
	columnUsedByPrefilter
	columnUsedHighSentinel
)

var columnUsageNames = map[columnUsage]string{
	columnUsedByAllBatches:        "allBatches",
	columnUsedByLiveBatches:       "liveBatches",
	columnUsedByFirstArchiveBatch: "firstArchiveBatch",
	columnUsedByLastArchiveBatch:  "lastArchiveBatch",
	columnUsedByPrefilter:         "prefilter",
}

func (u columnUsage) MarshalJSON() ([]byte, error) {
	var usageStrings []string
	for mask := columnUsedByAllBatches; mask < columnUsedHighSentinel; mask <<= 1 {
		usage := u & mask
		if usage != 0 {
			usageStrings = append(usageStrings, columnUsageNames[usage])
		}
	}
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(strings.Join(usageStrings, "+"))
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// TableScanner defines how data for a table should be fed to device memory for
// processing (scanner in a traditional terminology).
type TableScanner struct {
	// Snapshot of the table schema for convenience.
	Schema *memstore.TableSchema `json:"-"`
	// IDS of all table shards to be scanned on this instance.
	Shards []int `json:"shards"`
	// IDs of columns to be used in this query, in the following order:
	//   1. Columns not from ArchivingSortColumns.
	//   2. Columns from ArchivingSortColumns in reverse order.
	Columns []int `json:"columns"`
	// reversed mapping from columnID to column scan order index
	ColumnsByIDs map[int]int `json:"-"`

	// Map from column ID to its usage by the query.
	ColumnUsages map[int]columnUsage `json:"columnUsage"`

	// Fact table specifics:

	// Values of equality prefilters in order. Each 4 bytes of the uint32 is used
	// to store any data type other than UUID (not supported).
	EqualityPrefilterValues []uint32 `json:"equalityPrefilterValues,omitempty"`
	// Boundary types and values of the final range prefilter.
	RangePrefilterBoundaries [2]boundaryType `json:"rangePrefilterBoundaries"`
	RangePrefilterValues     [2]uint32       `json:"rangePrefilterValues"`
	// Range of archive batches to process: [Start, end).
	// Depending on the archiving progress of each shard, live batches may be
	// skipped for processing if the archiving cutoff is after the time of
	// ArchiveBatchIDEnd.
	ArchiveBatchIDStart int `json:"archiveBatchIDStart"`
	ArchiveBatchIDEnd   int `json:"archiveBatchIDEnd"`
}

// foreignTables stores foreignTables data
type foreignTable struct {
	// batches[batchIndex][columnIndex]
	// batchIndex = batchID - BaseBatchID
	// columnIndex corresponds to columnIndex in TableScanner columns order
	batches               [][]deviceVectorPartySlice
	numRecordsInLastBatch int
	// stores the remote join column in main table
	remoteJoinColumn *expr.VarRef
	// primary key data at host.
	hostPrimaryKeyData  memstore.PrimaryKeyData
	devicePrimaryKeyPtr devicePointer
}

// deviceVectorPartySlice stores pointers to data for a column in device memory.
type deviceVectorPartySlice struct {
	values devicePointer
	nulls  devicePointer
	// The length of the count vector is Length+1, similar to memstore.VectorParty
	counts devicePointer
	// Used only by device column. We allocate device memory deviceManagerOnce for counts, nulls
	// and values vector and when free we free only the base pointer. The memory layout
	// is counts,nulls,values and for counts vector, we will not copy the 64 bytes padding.
	basePtr   devicePointer
	length    int
	valueType memCom.DataType
	// pointer to default value from schema
	defaultValue    memCom.DataValue
	valueStartIndex int
	nullStartIndex  int
	countStartIndex int
}

// oopkBatchContext stores context for the current batch being processed by
// one-operator-per-kernel execution. For simplicity OOPK only supports data
// width up to 32 bit.
type oopkBatchContext struct {
	// For convenience purpose.
	device int

	// Input data according to TableScanner.Columns order.
	columns []deviceVectorPartySlice

	// pointer to Columns[firstColumn]'s count vector
	baseCountD devicePointer
	// startRow when firstColumn has no count vector
	startRow int
	// Index for permuting elements in raw column values. Also filter will be applied on
	// index vector instead on value vectors.
	indexVectorD devicePointer
	// Space for storing filter values. True value means we will keep the row.
	// We will reuse this space for all filter processing.
	predicateVectorD devicePointer
	// geo predicate vector
	geoPredicateVectorD devicePointer
	// foreignTableRecordIDsD holds recordIDs for related to each foreign table
	// to address the recordIDVector for foreignTable with tableID x, use foreignTableRecordIDsD[x-1]
	foreignTableRecordIDsD []devicePointer

	// timezoneLookupD points to an array of timezone offsets in seconds indexed by timezone string enum
	timezoneLookupD     devicePointer
	timezoneLookupDSize int

	// Remaining number of inputs in indexVectorD after filtering.
	// Notice that this size is not necessarily number of database rows
	// when columns[0] is compressed.
	size               int
	sizeAfterPreFilter int

	// Scratch vectors for evaluating the current AST expr in device memory.
	// [0] stores the values and [1] stores the validities (NULLs).
	// The data width of each value is always 4 bytes.
	// The data width of each validity (NULL) is always 1 byte.
	// Values and validities of each stack frame are allocated together,
	// with the validity array following the value array.
	// The length of each vector is size (same as indexVectorD).
	exprStackD [][2]devicePointer

	// Input and output storage in device memory before and after sort-reduce-by-key.
	// The capacity of the dimension and measure vector should be at least
	// resultSize+size.
	// First resultSize records stores results from processing prior batches.
	// Followed by size records from the current batch.
	// Sort and reduce by key will operate on all resultSize+size records.
	//
	// Because reduce_by_key outputs to separate buffers, we need to alternate two
	// sets of dimension and measure buffers for input and output.
	// We store the input buffer in [0], and the output buffer in [1] for the
	// following dimensionVectorH and measureVectorH.

	// one giant dimension vector
	// that contains each dimension columnar vector
	// ordered in the following order:
	// 4 byte dimensions -> 2 byte dimensions -> 1 byte dimensions (including validity vector).
	dimensionVectorD [2]devicePointer
	// hash vector is used to store the 64bit hash value hashed from
	// dimension row (combining all dimension values into one byte array)
	// generated in sort, used in sort and reduce
	hashVectorD [2]devicePointer
	// dimIndexVectorD is different from index vector,
	// it is the index of dimension vector
	// its length is the resultSize from previous batches + size of current batch
	dimIndexVectorD [2]devicePointer

	// Each element stores a 4 byte measure value.
	// Except SUM that uses 8 bytes
	measureVectorD [2]devicePointer

	// For aggregate queries: Size of the results from prior batches.
	// For non aggregate queries: result size for current batch, after decompression
	resultSize int

	// Capacity of the result dimension and measure vector, should be at least
	// resultSize+size.
	resultCapacity int

	// Query execution stats for current batch.
	stats oopkBatchStats
}

// OOPKContext defines additional query context for one-operator-per-kernel
// execution.
type OOPKContext struct {
	// Compiled and annotated filters.
	// The filters are converted to CNF equivalent so that AND does not exist in
	// any underlying expr.Expr any more.

	// Filters that apply to all archive and live batches.
	// MainTableCommonFilters match filters with only main table columns involved
	MainTableCommonFilters []expr.Expr `json:"mainTableCommonFilters,omitempty"`
	// ForeignTableCommonFilters match filters with foreign table columns involved
	ForeignTableCommonFilters []expr.Expr `json:"foreignTableCommonFilters,omitempty"`
	// Lower bound [0] and upper bound [1] time filter. nil if not applicable.
	// [0] should be applied to the first archive batch and all live batches.
	// [1] should be applied to the last archive batch and all live batches.
	TimeFilters [2]expr.Expr `json:"timeFilters"`
	// Prefilters that only apply to live batches.
	// Archiving cutoff filtering is processed directly by the query engine and not
	// included here (different shards may have different cutoffs).
	Prefilters []expr.Expr `json:"prefilters,omitempty"`

	// Compiled and annotated ASTs for dimensions and measure.
	Dimensions []expr.Expr `json:"dimensions"`
	// Index of single dimension vector in global dimension vector
	// Following sorted order based on bytes
	DimensionVectorIndex []int `json:"dimensionVectorIndex"`
	// Number of dimensions per dim width
	NumDimsPerDimWidth queryCom.DimCountsPerDimWidth `json:"numDims"`
	// Dim row bytes is the sum number of bytes of all dimension values
	// plus validity bytes, for memory allocation convenience
	DimRowBytes int `json:"dimRowBytes"`

	// For one-operator-per-kernel we only support one measure per query.
	Measure       expr.Expr                `json:"measure"`
	MeasureBytes  int                      `json:"measureBytes"`
	AggregateType C.enum_AggregateFunction `json:"aggregate"`

	// Storage for current batch.
	currentBatch oopkBatchContext

	// foreignTables holds the batches for each foreign table
	// to address each foreignTable with tableID x, use foreignTables[x-1]
	// nil foreignTable means not an actual foreign table join.
	foreignTables []*foreignTable

	// nil means no geo intersection
	geoIntersection *geoIntersection

	// Result storage in host memory. The format is the same as the dimension and
	// measure vector in oopkBatchContext.
	dimensionVectorH unsafe.Pointer
	measureVectorH   unsafe.Pointer
	// hllVectorD stores hll dense or sparse vector in device memory.
	hllVectorD devicePointer
	// size of hll vector
	hllVectorSize int64
	// hllDimRegIDCountD stores regID count for each dim in device memory.
	hllDimRegIDCountD devicePointer
	ResultSize        int `json:"resultSize"`

	// For reporting purpose only.
	DeviceMemoryRequirement int           `json:"deviceMem"`
	DurationWaitedForDevice time.Duration `json:"durationWaitedForDevice"`

	// Stores the overall query stats for live batches and archive batches.
	LiveBatchStats    oopkQueryStats `json:"liveStats"`
	ArchiveBatchStats oopkQueryStats `json:"archiveStats"`

	// indicate query can be return in the middle, no need to process all batches,
	// this is usually for non-aggregation query with limit condition
	done bool
}

// timezoneTableContext stores context for timezone column queries
type timezoneTableContext struct {
	tableAlias  string
	tableColumn string
}

// context for processing dimensions
type resultFlushContext struct {
	// caches time formatted time dimension values
	dimensionValueCache []map[queryCom.TimeDimensionMeta]map[int64]string
	dimensionDataTypes  []memCom.DataType
	reverseDicts        map[int][]string
}

// GeoIntersection is the struct to storing geo intersection related fields.
type geoIntersection struct {
	// Following fields are generated by compiler.
	// Geo tableID (scanner id)
	shapeTableID int
	// ID of the shape column.
	shapeColumnID int
	// Table ID of geo point.
	pointTableID int
	// ID of the point column in main table.
	pointColumnID int
	// List of shape uuids.
	shapeUUIDs []string
	// check point in shape or not
	inOrOut bool
	// dimIndex is the geo dimension index
	// if <0, meaning there is no dimension for geo
	// and this query only has geo filter
	dimIndex int

	// Following fields are generated by processor
	shapeLatLongs devicePointer
	shapeIndexs   devicePointer
	// map from shape index to index of shapeUUID
	validShapeUUIDs []string
	numShapes       int
	totalNumPoints  int
}

// AQLQueryContext stores all contextual data for handling an AQL query.
type AQLQueryContext struct {
	// The query input.
	Query *AQLQuery `json:"query"`

	// Context for one-operator-per-kernel execution.
	OOPK OOPKContext `json:"oopk"`

	//// Compiled time series aggregate query structure.
	//// TODO: TSAggregate is only used for VM based query engine.
	//TSAggregate C.TimeSeriesAggregate `json:"-"`

	// Scanner for all tables. [0] for the main table; [1:] for tables in joins.
	TableScanners []*TableScanner `json:"scanners"`
	// Map from table alias to ID (index to TableScanners).
	TableIDByAlias map[string]int `json:"tableIDs"`
	// Map from table name to schema for convenience. In case of self join,
	// only one entry is referenced here by the name of the table.
	TableSchemaByName map[string]*memstore.TableSchema `json:"-"`
	// Index to filters in Query.Filters that are identified as prefilters.
	Prefilters []int `json:"prefilters,omitempty"`

	Error error `json:"error,omitempty"`

	Device int `json:"device"`

	Debug bool `json:"debug,omitempty"`

	Profiling string `json:"profiling,omitempty"`

	// We alternate with two Cuda streams between batches for pipelining.
	// [0] stores the current stream, and [1] stores the other stream.
	cudaStreams [2]unsafe.Pointer

	Results            queryCom.AQLQueryResult `json:"-"`
	resultFlushContext resultFlushContext

	// whether to serialize the query result as HLLData. If ReturnHLLData is true, we will not release dimension
	// vector and measure vector until serialization is done.
	ReturnHLLData  bool   `json:"ReturnHLLData"`
	HLLQueryResult []byte `json:"-"`

	// for time filter
	fixedTimezone *time.Location
	fromTime      *alignedTime
	toTime        *alignedTime
	dstswitch     int64

	// timezone column and time filter related
	timezoneTable timezoneTableContext

	// fields for non aggregate query
	// Flag to indicate if this query is not aggregation query
	isNonAggregationQuery      bool
	numberOfRowsWritten        int
	maxBatchSizeAfterPrefilter int

	// for eager flush query result
	ResponseWriter http.ResponseWriter
}

// IsHLL return if the aggregation function is HLL
func (ctx *OOPKContext) IsHLL() bool {
	return ctx.AggregateType == C.AGGR_HLL
}

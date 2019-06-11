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

#ifndef QUERY_TIME_SERIES_AGGREGATE_H_
#define QUERY_TIME_SERIES_AGGREGATE_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "../cgoutils/utils.h"


// some macro used for host or device compilation
#ifdef RUN_ON_DEVICE
   #define SET_DEVICE(device) cudaSetDevice(device)
#else
   #define SET_DEVICE(device)
#endif

// These C-style array limits are used to make the query structure as flat as
// reasonably possible.
enum {
  MAX_FOREIGN_TABLES = 7,
  MAX_COLUMNS_OF_A_TABLE = 32,
  MAX_DIMENSIONS = 8,
  MAX_DIMENSION_BYTES = 32,
  MAX_MEASURES = 32,
  MAX_INSTRUCTIONS = 1024,
  HASH_BUCKET_SIZE = 8,
  HASH_STASH_SIZE = 4,
  HLL_BITS = 14,
  HLL_DENSE_SIZE = 1 << HLL_BITS,
  HLL_DENSE_THRESHOLD = HLL_DENSE_SIZE / 4,
  // 16-byte, 8-byte, 4-byte, 2-byte, 1-byte
  NUM_DIM_WIDTH = 5,
};

// All aggregate functions supported in time series aggregate queries.
enum AggregateFunction {
  AGGR_SUM_UNSIGNED = 1,
  AGGR_SUM_SIGNED = 2,
  AGGR_SUM_FLOAT = 3,
  AGGR_MIN_UNSIGNED = 4,
  AGGR_MIN_SIGNED = 5,
  AGGR_MIN_FLOAT = 6,
  AGGR_MAX_UNSIGNED = 7,
  AGGR_MAX_SIGNED = 8,
  AGGR_MAX_FLOAT = 9,
  AGGR_HLL = 10,
  AGGR_AVG_FLOAT = 11,
};

// All supported data type to covert golang vector to iterator.
enum DataType {
  Bool,
  Int8,
  Uint8,
  Int16,
  Uint16,
  Int32,
  Uint32,
  Float32,
  Int64,
  Uint64,
  Float64,
  GeoPoint,
  UUID,
};

// All supported constant data types.
enum ConstDataType {
  ConstInt,
  ConstFloat,
  ConstGeoPoint,
};

// All supported unary functor types.
enum UnaryFunctorType {
  Negate,
  Not,
  BitwiseNot,
  IsNull,
  IsNotNull,
  Noop,
  GetWeekStart,
  GetMonthStart,
  GetQuarterStart,
  GetYearStart,
  GetDayOfMonth,
  GetDayOfYear,
  GetMonthOfYear,
  GetQuarterOfYear,
  GetHLLValue,
};

// All supported binary functor types.
enum BinaryFunctorType {
  And,
  Or,
  Equal,
  NotEqual,
  LessThan,
  LessThanOrEqual,
  GreaterThan,
  GreaterThanOrEqual,
  Plus,
  Minus,
  Multiply,
  Divide,
  Mod,
  BitwiseAnd,
  BitwiseOr,
  BitwiseXor,
  Floor,
};

// RecordID
typedef struct {
  int32_t batchID;
  uint32_t index;
} RecordID;

// HashIndex stores the HashIndex
// For now we only support dimension table equal join
// the hash index will not support event time
typedef struct {
  uint8_t *buckets;
  uint32_t seeds[4];

  int keyBytes;
  int numHashes;
  int numBuckets;
} CuckooHashIndex;

// GeoPointT is the struct to represent a single geography point.
typedef struct {
  float Lat;
  float Long;
} GeoPointT;

// UUIDT is the struct to represent a 16-bytes UUID.
typedef struct {
  uint64_t p1;
  uint64_t p2;
} UUIDT;

typedef struct {
  bool HasDefault;
  union {
    bool BoolVal;
    int32_t Int32Val;
    uint32_t Uint32Val;
    float FloatVal;
    int64_t Int64Val;
    GeoPointT GeoPointVal;
    UUIDT UUIDVal;
  } Value;
} DefaultValue;

// VectorPartySlice stores the slice of each vector relevant to the query.
// It should be supplied for leaf nodes of the AST tree.
typedef struct {
  // Pointer points to memory allocated for this vp slice. We store counts,
  // nulls and values vector consecutively so that we can represent all
  // 3 pointers in the way like basePtr + 2 offsets.
  // If it's mode 0 vector, the BasePtr will be null.
  // If count vector does not present, the NullsOffset will be zero.
  // If null vector is not present, the ValuesOffset will be zero.
  uint8_t *BasePtr;
  uint32_t NullsOffset;
  uint32_t ValuesOffset;
  // Because of slicing and alignment, StartingIndex is not always 0.
  // StartingIndex will range from 0 to 7, with non-zero values only
  // used for bit-packed boolean values and nulls.
  uint8_t StartingIndex;

  // This is for converting the underlying pointer to appropriate pointer
  // type.
  enum DataType DataType;
  DefaultValue DefaultValue;

  uint32_t Length;
} VectorPartySlice;

// ScratchSpaceVector is the output vector for non-leaf non-root nodes and
// input vector for non-leaf nodes that have at least one non-leaf child.
typedef struct {
  uint8_t *Values;
  uint32_t NullsOffset;
  enum DataType DataType;
} ScratchSpaceVector;

// ConstantVector is the constant value in AST tree.
typedef struct {
  // Value from the AST tree.
  union {
    int32_t IntVal;
    float FloatVal;
    GeoPointT GeoPointVal;
  } Value;
  // Whether this values is valid.
  bool IsValid;
  // A constant indicate const type can be used.
  enum ConstDataType DataType;
} ConstantVector;

// ForeignColumnVector stores all batches of vectors for the target column
// and the record ids from hash lookup.
// Note: foreign vector are only from
// dimension tables and are unsorted columns from live batches.
typedef struct {
  RecordID *RecordIDs;
  VectorPartySlice *Batches;
  int32_t BaseBatchID;
  int32_t NumBatches;
  int32_t NumRecordsInLastBatch;
  int16_t *const TimezoneLookup;
  int16_t TimezoneLookupSize;
  enum DataType DataType;
  DefaultValue DefaultValue;
} ForeignColumnVector;

// All supported input vector type.
enum InputVectorType {
  VectorPartyInput,
  ScratchSpaceInput,
  ConstantInput,
  ForeignColumnInput
};

// InputVector is the vector used as input to transform and filter. Actual
// type of input is decided by the type field.
typedef struct {
  union {
    ConstantVector Constant;
    VectorPartySlice VP;
    ScratchSpaceVector ScratchSpace;
    ForeignColumnVector ForeignVP;
  } Vector;
  enum InputVectorType Type;
} InputVector;

// DimensionVector stores the dimension vector byte array.
// DimensionVector will be allocated together.
// The layout will be 4byte vectors followed by 2 byte vectors followed by 1
// byte vectors and null vector (1 byte) Note: IndexVector is the indexVector of
// dimension vector, not the index vector of the batch Sort and reduce will be
// first done on IndexVector and than permutated to the output dimension vectors
typedef struct {
  uint8_t *DimValues;
  uint64_t *HashValues;
  uint32_t *IndexVector;
  int VectorCapacity;
  uint8_t NumDimsPerDimWidth[NUM_DIM_WIDTH];
} DimensionVector;

// DimensionOutputVector is used as the output vector of dimension
// transformation for each dimension.
typedef struct {
  uint8_t *DimValues;
  uint8_t *DimNulls;
  enum DataType DataType;
} DimensionOutputVector;

// MeasureOutputVector is used as output vector of measure transformation.
// Right now we have one measure per query but we may have multiple measures
// in future.
typedef struct {
  // Where to write the values
  uint32_t *Values;
  enum DataType DataType;
  enum AggregateFunction AggFunc;
} MeasureOutputVector;

// All supported output vector type.
enum OutputVectorType {
  ScratchSpaceOutput,
  MeasureOutput,
  DimensionOutput,
};

// OutputVector is the vector used as output of transform. Actual
// type of input is decided by the type field.
typedef struct {
  union {
    ScratchSpaceVector ScratchSpace;
    DimensionOutputVector Dimension;
    MeasureOutputVector Measure;
  } Vector;
  enum OutputVectorType Type;
} OutputVector;

/*
// Batch stores all columns of the batch relevant to the query.
//
// Columns from different batches of the same table share the same order:
//   1. Columns not from ArchivingSortColumns.
//   2. Columns from ArchivingSortColumns in reverse order.
typedef struct {
  VectorPartySlice Columns[MAX_COLUMNS_OF_A_TABLE];
} Batch;

// ForeignTable stores the data and join conditions of a table being joined.
// A hash index on the equi-join columns is either maintained or created at
// query time before the query executes.
typedef struct {
  // Input data:
  Batch *batches;
  int32_t BaseBatchID;
  int32_t NumBatches;

  // Column metadata:
  int32_t NumColumns;
  uint16_t ValueBitsByColumn[MAX_COLUMNS_OF_A_TABLE];

  // TODO(shengyue): hash index
  CuckooHashIndex HashIndex;

  // Equi-join conditions:
  int32_t NumberOfEqualityPairs;
  // Index of each column in batches[x].Columns used in the equi-join
  // condition.
  uint8_t LocalColumns[MAX_COLUMNS_OF_A_TABLE];
  // Index of each remote column in the global column array used in the
  // equi-join condition. All columns from the main table are added to the
  // global column array, followed by all columns from the first foreign
  // table, followed by all columns from the second foreign table..
  // All remote columns must be present on the global column array by the time
  // this join happens; the values of which are used as the hash key to locate
  // the matching record on this table.
  uint8_t RemoteColumns[MAX_COLUMNS_OF_A_TABLE];
} ForeignTable;

// TimeSeriesAggregate defines the query, stores the input data from
// foreign tables, and manages output buffers. Input data from the main table
// are passed to Cuda separately to allow batch-based pipelining while reusing
// the same query instance across multiple batches.
typedef struct {
  // Foreign tables with input data and hash index:
  ForeignTable ForeignTables[MAX_FOREIGN_TABLES];
  int32_t NumForeignTables;

  // Column metadata for the main table:
  int32_t NumColumns;
  uint16_t ValueBitsByColumn[MAX_COLUMNS_OF_A_TABLE];

  // Query definition:
  int32_t NumDimensions;
  int32_t NumMeasures;
  // FilterInstsForUnsorted/ArchiveBatches produces 0 or 1 on top of the value
  // stack to indicate whether the record should be skipped or included for
  // all measures.
  uint32_t FilterInstsForLiveBatches[MAX_INSTRUCTIONS];
  uint32_t FilterInstsForArchiveBatches[MAX_INSTRUCTIONS];
  // DimMeasureInsts produces one uint32_t word for each dimension, followed
  // by one uint32_t word for each measure on top of the value stack.
  // It also produces one char for each dimension and measure on top of the
  // null stack to indicate whether they are valid (1) or null (0). A null
  // measure skips its aggregation. A null dimension is reported as null.
  uint32_t DimMeasureInsts[MAX_INSTRUCTIONS];
  // Actual word width (in bytes) of each dimension.
  uint8_t DimensionBytes[MAX_DIMENSIONS];
  // Total number of bytes of all dimensions.
  uint32_t TotalDimensionBytes;
  // Aggreate functions for each measure.
  enum AggregateFunction Aggregates[MAX_MEASURES];
} TimeSeriesAggregate;
*/

// GeoShape is the struct to represent a geofence shape. A single geoshape can
// consists of multiple polygons including holes inside the polygons. Lats and
// Longs vector stores the latitude and longitude values of each point of those
// polygons.
typedef struct {
  // Lats and Longs are stored in the format as
  //      [a1,a2,...an,a1,FLT_MAX,b1,bz,...bn]
  // where FLT_MAX denotes the beginning of next polygon.
  // We repeat the first vertex at end of each polygon, so the last line is
  // an-a1.
  float *Lats;
  float *Longs;
  // Number of coordinates including FLT_MAX placeholders  in the vector.
  uint16_t NumPoints;
} GeoShape;

// GeoShapeBatch
typedef struct {
  // last
  uint8_t *LatLongs;
  // 1. first one byte stores total number of words(uint32_t) needed to
  // store predicate value (in or out of shape) of each shape per point
  // each shape will take 1 bit so every 32 shapes will take 1 word
  // 2. next three bytes stores the total number of points
  int32_t TotalNumPoints;
  uint8_t TotalWords;
} GeoShapeBatch;

// unaryTransform defines the C transform interface for golang to call.
#ifdef __cplusplus
extern "C" {
#endif

// InitIndexVector initialize index vector
CGoCallResHandle InitIndexVector(uint32_t *indexVector,
                                 uint32_t start,
                                 int indexVectorLength,
                                 void *cudaStream,
                                 int device);

// HashLookup looks up input values and stores results (record ids) into record
// id vector
CGoCallResHandle HashLookup(InputVector input,
                            RecordID *output,
                            uint32_t *indexVector,
                            int indexVectorLength,
                            uint32_t *baseCounts,
                            uint32_t startCount,
                            CuckooHashIndex hashIndex,
                            void *cudaStream,
                            int device);

// UnaryTransform transforms an InputVector to output
// OutputVector by applying UnaryFunctor to each of the element.
// Output space should be preallocated by caller. Notice unaryTransform
// should not accept a constant, this should be ensured by the query
// compiler.
CGoCallResHandle UnaryTransform(InputVector input,
                                OutputVector output,
    // Will only be used at the leaf node and measure output.
                                uint32_t *indexVector,
                                int indexVectorLength,
    // Will only be used at the leaf node and measure output.
                                uint32_t *baseCounts,
                                uint32_t startCount,
                                enum UnaryFunctorType functorType,
                                void *cudaStream,
                                int device);

// UnaryFilter filters the index vector by applying the unary functor
// on input and uses the result as the filter predicate. It returns the
// index size after filtering. Caller is responsible for allocating
// and initializing the index vector. Filter will be in-place so
// no extra storage will be used.
CGoCallResHandle UnaryFilter(InputVector input,
                             uint32_t *indexVector,
                             uint8_t *predicateVector,
                             int indexVectorLength,
    // Will only be used for foreign table column input
                             RecordID **recordIDVectors,
                             int numForeignTables,
    // Will only be used if this is a leaf node.
                             uint32_t *baseCounts,
                             uint32_t startCount,
                             enum UnaryFunctorType functorType,
                             void *cudaStream,
                             int device);

// BinaryTransform applies BinaryFunctor on each pair of the elements in
// lhs and rhs and writes the output to OutputVector. Output space should
// be preallocated by caller. LHS and RHS cannot both be constant.
CGoCallResHandle BinaryTransform(InputVector lhs,
                                 InputVector rhs,
                                 OutputVector output,
    // Will only be used at the leaf node and measure output.
                                 uint32_t *indexVector,
                                 int indexVectorLength,
    // Will only be used at the leaf node and measure output.
                                 uint32_t *baseCounts,
                                 uint32_t startCount,
                                 enum BinaryFunctorType functorType,
                                 void *cudaStream,
                                 int device);

// BinaryFilter is the binary version of unaryFilter.
CGoCallResHandle BinaryFilter(InputVector lhs,
                              InputVector rhs,
                              uint32_t *indexVector,
                              uint8_t *predicateVector,
                              int indexVectorLength,
    // Will only be used for foreign table column input
                              RecordID **recordIDVectors,
                              int numForeignTables,
    // Will only be used if this is a leaf node.
                              uint32_t *baseCounts,
                              uint32_t startCount,
                              enum BinaryFunctorType functorType,
                              void *cudaStream,
                              int device);

// Sort performs a key-value sort to sort elements in keys and values in
// ascending key order. Notice we don't need data type of the measure
// values here since we only need to move the whole 4 bytes around
// without any change.
CGoCallResHandle Sort(DimensionVector keys,
                      int length,
                      void *cudaStream,
                      int device);

// Reduce reduces inputValues on inputKeys using the AggregateFunction and
// write the unique keys to outputKeys and aggregation results to outputValues.
// It returns number of unique keys as result also. Notice outputKeys and
// outputValues should be preallocated by caller.
CGoCallResHandle Reduce(DimensionVector inputKeys,
                        uint8_t *inputValues,
                        DimensionVector outputKeys,
                        uint8_t *outputValues,
                        int valueBytes,
                        int length,
                        enum AggregateFunction aggFunc,
                        void *cudaStream,
                        int device);

// HashReduce does the reduction using hash instead of sort and then reduce.
// Note for HashReduce we will not use hash vector for keys so since
// hash happens during hash map insertion, therefore we can skip allocation for
// hash vector. We will not need index vector as well since we no longer sort
// the rows.
CGoCallResHandle HashReduce(DimensionVector inputKeys,
                        uint8_t *inputValues,
                        DimensionVector outputKeys,
                        uint8_t *outputValues,
                        int valueBytes,
                        int length,
                        enum AggregateFunction aggFunc,
                        void *cudaStream,
                        int device);

// Expand function is used to decompress the dimensions which are compressed
// through baseCounts, and append to existing outputKeys.
// @inputKeys input DimensionVector
// @outputKeys output DimensionVector
// @baseCounts count vector for first column
// @indexVector index vector of the dimension keys
// @indexVectorLen length of index vector will be used for the output, it should
// be less or equal to length of keys in inputKeys
// outputOccupiedLen number of rows already in the outputKeys, as this will
// be append operation
CGoCallResHandle Expand(DimensionVector inputKeys,
                        DimensionVector outputKeys,
                        uint32_t *baseCounts,
                        uint32_t *indexVector,
                        int indexVectorLen,
                        int outputOccupiedLen,
                        void *cudaStream,
                        int device);

// HyperLogLog is the interface to do hyperloglog in one cgo function call.
// prevResultSize is to tell start position of keys and values of current batch.
// hllVector (dense/sparse) will only be set when we allocate it for the last
// batch. If it's the last batch. this function will return number of different
// dim value combinations. Otherwise it returns the (previous result size +
// number of unique hash values in current batch),
// hllVectorSizePtr stores the size of hllVector
// hllDimRegIDCount stores the num of reg_id per dim
CGoCallResHandle HyperLogLog(DimensionVector prevDimOut,
                             DimensionVector curDimOut,
                             uint32_t *prevValuesOut,
                             uint32_t *curValuesOut,
                             int prevResultSize,
                             int curBatchSize,
                             bool isLastBatch,
                             uint8_t **hllVectorPtr,
                             size_t *hllVectorSizePtr,
                             uint16_t **hllDimRegIDCountPtr,
                             void *cudaStream,
                             int device);

// GeoBatchIntersects is the interface to do geography intersections for all
// geoshapes altogether. inOrOut represents whether we want to check geo points
// are in the shape or not, 1 represents in and 0 represents not in.
// outputPredicate will be used for filter.
// Returns the filtered size otherwise returns the original size.
CGoCallResHandle GeoBatchIntersects(
    GeoShapeBatch geoShapeBatch, InputVector points, uint32_t *indexVector,
    int indexVectorLength, uint32_t startCount, RecordID **recordIDVectors,
    int numForeignTables, uint32_t *outputPredicate, bool inOrOut,
    void *cudaStream, int device);

// WriteGeoShapeDim is the interface to write the shape index to the dimension
// output. This method should be called after GeoBatchIntersects removes all
// non-intersecting rows. Note we need pass in the index vector length before
// geo since outputPredicate has not done compaction yet.
// Note:
//   1. we assume that the geo join will be many-to-one join
//   2. we only support IN operation for geo intersects join
CGoCallResHandle WriteGeoShapeDim(
    int shapeTotalWords, DimensionOutputVector dimOut,
    int indexVectorLengthBeforeGeo, uint32_t *outputPredicate,
    void *cudaStream, int device);

// BoostrapDevice will bootstrap the all gpu devices with approriate actions.
// For now it will just initialize the constant memory.
CGoCallResHandle BootstrapDevice();
#ifdef __cplusplus
}
#endif

#endif  // QUERY_TIME_SERIES_AGGREGATE_H_

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
	"math"
	"math/rand"
	"unsafe"

	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memutils"
	"github.com/uber/aresdb/utils"
	"sync"
	"time"
)

const (
	bucketSize = 8
	// log2(numHashes)
	log2NumHashes = 2
	// number of hash functions
	numHashes = 1 << log2NumHashes
	// size of the stash
	stashSize = 4
	// only when load factor is larger than resizeThreshold
	// we do resize without rehashing first
	resizeThreshold = 0.9
	// growth percentage for each resize
	resizeFactor float32 = 0.2

	// reserve signature 0 to indicate the slot is empty
	emptySignature = 0
	minSignature   = 1

	// offsets for bucket
	// the data layout in a bucket is the following manner
	// RecordID[8]|signature[8]|eventTime[8](optional)|key[8]
	offsetToSignature           = bucketSize * recordIDBytes
	offsetToEventTime           = offsetToSignature + bucketSize*1
	offsetToKeyWithEventTime    = offsetToEventTime + bucketSize*4
	offsetToKeyWithoutEventTime = offsetToEventTime
)

type stashEntry struct {
	isValid   bool
	eventTime uint32
	key       Key
	value     RecordID
}

type hashResult struct {
	bucket    unsafe.Pointer
	signature uint8
}

// CuckooIndex is a implementation of Hash Index using Cuckoo Hashing algorithm
// Lazy expiration is used to invalidate expired items
// CuckooIndex is not threadsafe
type CuckooIndex struct {
	// number of bytes of a key
	keyBytes int
	// the size in bytes each bucket takes
	bucketBytes int
	// number of buckets
	numBuckets int
	// number of entries in bucket
	numBucketEntries uint
	// number of entries in stash
	numStashEntries uint
	// maxTrials when do evict and add
	maxTrials int

	// mark whether it is a fact vs dimension table hash index
	hasEventTime bool
	// bucket array
	// the array is a byte array allocated in c
	// the data layout in a bucket is the following manner
	// RecordID[8]|signature[8]|eventTime[8](optional)|key[8]
	buckets unsafe.Pointer
	// stash is a special bucket, the only difference is its size
	// use of stash is to reduce the probability of rehashing
	// Note stash memory is allocated/de-allocated together with buckets
	// stash also have 8 slots, only 4 (configurable) of them are used
	stash unsafe.Pointer

	// extra stash entry in go struct
	// act as a temporary place before resize
	staging *stagingEntry
	// seeds holds the hash function seeds
	// use different seeds to generate different hash values
	seeds [numHashes]uint32

	// eventTimeCutoff record the smallest timestamp that was
	eventTimeCutoff uint32

	rand *rand.Rand

	// report change of unmanaged memory.
	hostMemoryManager common.HostMemoryManager

	// mutex protects internal buffer for GPU transfer
	transferLock sync.RWMutex
}

type stagingEntry struct {
	eventTime uint32
	key       Key
	value     RecordID
}

func getDefaultInitNumBuckets() int {
	if utils.IsTest() {
		return 1000
	}
	return 1000000
}

// Size returns the current number of items stored in the hash table
// including expired items yet not known to the system
func (c *CuckooIndex) Size() uint {
	return c.numBucketEntries + c.numStashEntries
}

// Update updates a key with a new recordID. Return whether key exists in the primary key or not.
func (c *CuckooIndex) Update(key Key, value RecordID) bool {
	c.transferLock.Lock()
	defer c.transferLock.Unlock()
	keyPtr := unsafe.Pointer(&key[0])
	for hashIndex := 0; hashIndex < numHashes; hashIndex++ {
		hashResult := c.hash(keyPtr, hashIndex)
		for i := 0; i < bucketSize; i++ {
			existingSignature := *c.getSignature(hashResult.bucket, i)
			if !c.recordExpired(hashResult.bucket, i) &&
				existingSignature == hashResult.signature &&
				utils.MemEqual(c.getKey(hashResult.bucket, i), keyPtr, c.keyBytes) {
				*c.getRecordID(hashResult.bucket, i) = value
				return true
			}
		}
	}

	for i := 0; i < stashSize; i++ {
		if !c.isEmpty(c.stash, i) &&
			!c.recordExpired(c.stash, i) &&
			utils.MemEqual(c.getKey(c.stash, i), keyPtr, c.keyBytes) {
			*c.getRecordID(c.stash, i) = value
			return true
		}
	}
	return false
}

// Find looks up a record given key
func (c *CuckooIndex) Find(key Key) (RecordID, bool) {
	keyPtr := unsafe.Pointer(&key[0])
	for hashIndex := 0; hashIndex < numHashes; hashIndex++ {
		hashResult := c.hash(keyPtr, hashIndex)
		for i := 0; i < bucketSize; i++ {
			existingSignature := *c.getSignature(hashResult.bucket, i)
			if !c.recordExpired(hashResult.bucket, i) &&
				existingSignature == hashResult.signature &&
				utils.MemEqual(c.getKey(hashResult.bucket, i), keyPtr, c.keyBytes) {
				return *c.getRecordID(hashResult.bucket, i), true
			}
		}
	}

	for i := 0; i < stashSize; i++ {
		if !c.isEmpty(c.stash, i) &&
			!c.recordExpired(c.stash, i) &&
			utils.MemEqual(c.getKey(c.stash, i), keyPtr, c.keyBytes) {
			return *c.getRecordID(c.stash, i), true
		}
	}
	return RecordID{}, false
}

// Capacity returns how many items current primary key can hold.
func (c *CuckooIndex) Capacity() uint {
	return uint(c.numBuckets * bucketSize)
}

// AllocatedBytes returns the allocated size of primary key in bytes.
func (c *CuckooIndex) AllocatedBytes() uint {
	return c.allocatedBytes()
}

// FindOrInsert find the existing key or insert a new (key, value) pair
func (c *CuckooIndex) FindOrInsert(key Key, value RecordID, eventTime uint32) (existingFound bool, recordID RecordID, err error) {
	c.transferLock.Lock()
	defer c.transferLock.Unlock()
	if c.eventTimeExpired(eventTime) {
		return false, RecordID{}, utils.StackError(nil, "Stale Value, eventTimeCutOff: %d, getEventTime Inserted: %d", c.eventTimeCutoff, eventTime)
	}

	existingFound, added, recordID, hashResults := c.findOrAddNew(unsafe.Pointer(&key[0]), value, eventTime)
	if existingFound || added {
		return existingFound, recordID, nil
	}

	if c.cuckooAdd(unsafe.Pointer(&key[0]), value, eventTime, hashResults) {
		return false, value, nil
	}

	for i := 1; ; i++ {
		if c.resize(float32(i) * resizeFactor) {
			return false, value, nil
		}
	}
}

// Delete will delete a item with given key
func (c *CuckooIndex) Delete(key Key) {
	c.transferLock.Lock()
	defer c.transferLock.Unlock()
	var hashResults [numHashes]hashResult
	for hashIndex := 0; hashIndex < numHashes; hashIndex++ {
		hashResult := c.hash(unsafe.Pointer(&key[0]), hashIndex)
		hashResults[hashIndex] = hashResult
		for i := 0; i < bucketSize; i++ {
			if *c.getSignature(hashResult.bucket, i) == hashResult.signature &&
				utils.MemEqual(c.getKey(hashResult.bucket, i), unsafe.Pointer(&key[0]), c.keyBytes) {
				c.numBucketEntries--
				*c.getSignature(hashResult.bucket, i) = emptySignature
				return
			}
		}
	}

	for i := 0; i < stashSize; i++ {
		if !c.isEmpty(c.stash, i) &&
			utils.MemEqual(c.getKey(c.stash, i), unsafe.Pointer(&key[0]), c.keyBytes) {
			*c.getSignature(c.stash, i) = emptySignature
			c.numStashEntries--
			return
		}
	}
}

// UpdateEventTimeCutoff updates eventTimeCutoff
func (c *CuckooIndex) UpdateEventTimeCutoff(cutoff uint32) {
	c.eventTimeCutoff = cutoff
}

// GetEventTimeCutoff returns the cutoff event time.
func (c *CuckooIndex) GetEventTimeCutoff() uint32 {
	return c.eventTimeCutoff
}

// LockForTransfer locks primary key for transfer and returns PrimaryKeyData
func (c *CuckooIndex) LockForTransfer() PrimaryKeyData {
	c.transferLock.RLock()
	return PrimaryKeyData{
		Data: c.buckets,
		// numBuckets plus stash bucket
		NumBytes:   c.bucketBytes * (c.numBuckets + 1),
		Seeds:      c.seeds,
		KeyBytes:   c.keyBytes,
		NumBuckets: c.numBuckets,
	}
}

// UnlockAfterTransfer release transfer lock
func (c *CuckooIndex) UnlockAfterTransfer() {
	c.transferLock.RUnlock()
}

func (c *CuckooIndex) hash(key unsafe.Pointer, index int) hashResult {

	hashValue := utils.Murmur3Sum32(key, c.keyBytes, c.seeds[index])

	bucketIndex := hashValue % uint32(c.numBuckets)
	bucket := utils.MemAccess(c.buckets, int(bucketIndex)*c.bucketBytes)
	signature := c.extractSignatureByte(hashValue)

	return hashResult{
		bucket:    bucket,
		signature: signature,
	}
}

func (c *CuckooIndex) generateRandomSeeds() {
	for i := range c.seeds {
		c.seeds[i] = c.rand.Uint32()
	}
}

func (c *CuckooIndex) loadFactor() float64 {
	return float64(c.numBucketEntries+c.numStashEntries) / float64(c.numBuckets*bucketSize)
}

// extractSignatureByte get the most significant byte from the hash value
// this is to avoid comparison of byte array as much as possible, which is expensive
func (c *CuckooIndex) extractSignatureByte(hashValue uint32) uint8 {
	signature := uint8(hashValue >> (32 - 8))
	if signature < minSignature {
		signature = minSignature
	}
	return signature
}

func (c *CuckooIndex) getSignature(bucket unsafe.Pointer, index int) *uint8 {
	return (*uint8)(utils.MemAccess(bucket, offsetToSignature+index))
}

func (c *CuckooIndex) getRecordID(bucket unsafe.Pointer, index int) *RecordID {
	return (*RecordID)(utils.MemAccess(bucket, index*recordIDBytes))
}

// call should be aware there is no eventime present, this method will return incorrect
func (c *CuckooIndex) getEventTime(bucket unsafe.Pointer, index int) *uint32 {
	return (*uint32)(utils.MemAccess(bucket, offsetToEventTime+index*4))
}

func (c *CuckooIndex) getKey(bucket unsafe.Pointer, index int) unsafe.Pointer {
	if !c.hasEventTime {
		return utils.MemAccess(bucket, offsetToKeyWithoutEventTime+index*c.keyBytes)
	}
	return utils.MemAccess(bucket, offsetToKeyWithEventTime+index*c.keyBytes)
}

func (c *CuckooIndex) isEmpty(bucket unsafe.Pointer, index int) bool {
	return *c.getSignature(bucket, index) == emptySignature
}

func (c *CuckooIndex) recordExpired(bucket unsafe.Pointer, index int) bool {
	return c.hasEventTime && c.eventTimeExpired(*c.getEventTime(bucket, index))
}

func (c *CuckooIndex) eventTimeExpired(eventTime uint32) bool {
	return c.eventTimeCutoff > eventTime
}

// randomSwap randomly pick a bucket position and swap with the value
func (c *CuckooIndex) randomSwap(key unsafe.Pointer, recordID *RecordID, eventTime *uint32, hashResults [numHashes]hashResult) {
	hashResult := hashResults[c.rand.Intn(numHashes)]
	slotIndex := c.rand.Intn(bucketSize)

	*c.getRecordID(hashResult.bucket, slotIndex), *recordID = *recordID, *c.getRecordID(hashResult.bucket, slotIndex)
	*c.getSignature(hashResult.bucket, slotIndex) = hashResult.signature
	memutils.MemSwap(c.getKey(hashResult.bucket, slotIndex), key, c.keyBytes)

	if c.hasEventTime {
		*c.getEventTime(hashResult.bucket, slotIndex), *eventTime = *eventTime, *c.getEventTime(hashResult.bucket, slotIndex)
	}
}

// addNew only attempts to add new item into buckets, but not stash
// and assume there is no existing item
// and will not do the cuckoo process when the process fail
func (c *CuckooIndex) addNew(key unsafe.Pointer, recordID RecordID, eventTime uint32) (added bool, hashResults [numHashes]hashResult) {
	for hashIndex := 0; hashIndex < numHashes; hashIndex++ {
		hashResult := c.hash(key, hashIndex)
		hashResults[hashIndex] = hashResult
		for i := 0; i < bucketSize; i++ {
			if c.isEmpty(hashResult.bucket, i) {
				c.insertBucket(key, recordID, hashResult.signature, eventTime, hashResult.bucket, i)
				c.numBucketEntries++
				added = true
				return
			} else if c.recordExpired(hashResult.bucket, i) {
				c.insertBucket(key, recordID, hashResult.signature, eventTime, hashResult.bucket, i)
				added = true
				return
			}
		}
	}
	return
}

// find existing or add new item to available slot
func (c *CuckooIndex) findOrAddNew(key unsafe.Pointer, value RecordID, eventTime uint32) (existingFound bool, added bool, recordID RecordID, hashResults [numHashes]hashResult) {
	indexToInsert := -1
	isInsert := false
	var bucketToInsert unsafe.Pointer
	var signatureToInsert uint8

	// look for existing record in buckets with all hash functions
	// mark potential slot to insert new record
	for hashIndex := 0; hashIndex < numHashes; hashIndex++ {
		hashResult := c.hash(key, hashIndex)
		hashResults[hashIndex] = hashResult
		for i := 0; i < bucketSize; i++ {
			isEmpty := c.isEmpty(hashResult.bucket, i)
			if isEmpty || c.recordExpired(hashResult.bucket, i) {
				if indexToInsert < 0 {
					indexToInsert = i
					bucketToInsert = hashResult.bucket
					signatureToInsert = hashResult.signature
					isInsert = isEmpty
				}
			} else if *c.getSignature(hashResult.bucket, i) == hashResult.signature && utils.MemEqual(c.getKey(hashResult.bucket, i), key, c.keyBytes) {
				existingFound = true
				recordID = *c.getRecordID(hashResult.bucket, i)
				return
			}
		}
	}

	// look for existing record in stash
	for i := 0; i < stashSize; i++ {
		if !c.recordExpired(c.stash, i) && !c.isEmpty(c.stash, i) &&
			utils.MemEqual(c.getKey(c.stash, i), key, c.keyBytes) {
			existingFound = true
			recordID = *c.getRecordID(c.stash, i)
			return
		}
	}

	if indexToInsert >= 0 {
		c.insertBucket(key, value, signatureToInsert, eventTime, bucketToInsert, indexToInsert)
		if isInsert {
			c.numBucketEntries++
		}
		added = true
		recordID = value
	}

	return
}

// randomly evict existing item with conflict hash and reinsert
func (c *CuckooIndex) cuckooAdd(key unsafe.Pointer, recordID RecordID, eventTime uint32, hashResults [numHashes]hashResult) bool {
	insertKey := make(Key, c.keyBytes)
	memutils.MemCopy(unsafe.Pointer(&insertKey[0]), key, c.keyBytes)

	for trial := 0; trial < c.maxTrials; trial++ {
		c.randomSwap(unsafe.Pointer(&insertKey[0]), &recordID, &eventTime, hashResults)
		added, evictHashResults := c.addNew(unsafe.Pointer(&insertKey[0]), recordID, eventTime)
		if added {
			return true
		}
		hashResults = evictHashResults
	}

	// insert to stash
	for i := 0; i < stashSize; i++ {
		if c.isEmpty(c.stash, i) {
			c.numStashEntries++
			c.insertBucket(unsafe.Pointer(&insertKey[0]), recordID, minSignature, eventTime, c.stash, i)
			return true
		} else if c.recordExpired(c.stash, i) {
			c.insertBucket(unsafe.Pointer(&insertKey[0]), recordID, minSignature, eventTime, c.stash, i)
			return true
		}
	}

	// save swapped out record to staging area for resizing
	if c.staging == nil {
		staging := stagingEntry{}
		if c.hasEventTime {
			staging.eventTime = eventTime
		}
		staging.value = recordID
		staging.key = make(Key, c.keyBytes)
		copy(staging.key, insertKey)
		c.staging = &staging
	}

	return false
}

// insert will insert without find existing item
func (c *CuckooIndex) insert(key unsafe.Pointer, v RecordID, eventTime uint32) bool {
	added, hashResults := c.addNew(key, v, eventTime)
	if !added {
		return c.cuckooAdd(key, v, eventTime, hashResults)
	}
	return true
}

func (c *CuckooIndex) insertBucket(key unsafe.Pointer, recordID RecordID, signature uint8, eventTime uint32, bucket unsafe.Pointer, index int) {
	memutils.MemCopy(c.getKey(bucket, index), key, c.keyBytes)
	*c.getSignature(bucket, index) = signature
	*c.getRecordID(bucket, index) = recordID
	if c.hasEventTime {
		*c.getEventTime(bucket, index) = eventTime
	}
}

func (c *CuckooIndex) getMaxTrials() int {
	return int(1+math.Log2(float64(c.numBuckets))) * 2
}

// resize the hast table by growFactor
func (c *CuckooIndex) resize(resizeFactor float32) (ok bool) {
	newIndex := newCuckooIndex(
		c.keyBytes,
		c.hasEventTime,
		int(float32(c.numBuckets)*float32(1+resizeFactor)),
		c.hostMemoryManager,
	)

	// insert existing keys to new index
	for i := 0; i < c.numBuckets+1; i++ {
		var bucket unsafe.Pointer
		numElements := bucketSize
		if i == c.numBuckets {
			bucket = c.stash
			numElements = stashSize
		} else {
			bucket = utils.MemAccess(c.buckets, i*c.bucketBytes)
		}

		for j := 0; j < numElements; j++ {
			if c.isEmpty(bucket, j) || c.recordExpired(bucket, j) {
				continue
			}

			eventTime := *c.getEventTime(bucket, j)
			key := c.getKey(bucket, j)
			recordID := *c.getRecordID(bucket, j)

			if newIndex.insert(key, recordID, eventTime) {
				continue
			}

			newIndex.Destruct()
			return false
		}
	}

	if c.staging != nil {
		if !newIndex.insert(unsafe.Pointer(&c.staging.key[0]), c.staging.value, c.staging.eventTime) {
			newIndex.Destruct()
			return false
		}
		newIndex.staging = nil
	}

	// copy to current index. note transferLock is reused.
	memutils.HostFree(c.buckets)
	newIndex.hostMemoryManager.ReportUnmanagedSpaceUsageChange(int64(-c.allocatedBytes()))

	c.numBuckets = newIndex.numBuckets
	c.rand = newIndex.rand
	c.seeds = newIndex.seeds
	c.buckets = newIndex.buckets
	c.staging = newIndex.staging
	c.stash = newIndex.stash
	c.numBucketEntries = newIndex.numBucketEntries
	c.numStashEntries = newIndex.numStashEntries
	c.maxTrials = newIndex.maxTrials

	newIndex = nil

	return true
}

func (c *CuckooIndex) allocatedBytes() uint {
	return uint(c.numBuckets * c.bucketBytes)
}

func (c *CuckooIndex) allocate() {
	totalBucketBytes := int(c.allocatedBytes())
	// allocate buckets plus stash
	c.buckets = memutils.HostAlloc(totalBucketBytes + c.bucketBytes)
	c.stash = utils.MemAccess(c.buckets, totalBucketBytes)
}

// newCuckooIndex create a cuckoo hashing index
func newCuckooIndex(keyBytes int, hasEventTime bool, initNumBuckets int,
	hostMemoryManager common.HostMemoryManager) *CuckooIndex {
	if initNumBuckets <= 0 {
		initNumBuckets = getDefaultInitNumBuckets()
	}

	cuckooIndex := &CuckooIndex{
		rand:              rand.New(rand.NewSource(time.Now().Unix())),
		keyBytes:          keyBytes,
		hasEventTime:      hasEventTime,
		numBuckets:        initNumBuckets,
		hostMemoryManager: hostMemoryManager,
		transferLock:      sync.RWMutex{},
	}

	// recordIDBytes + keyBytes + signature (1 byte)
	var cellBytes = recordIDBytes + cuckooIndex.keyBytes + 1
	// plus eventTime (4 bytes)
	if cuckooIndex.hasEventTime {
		cellBytes += 4
	}
	cuckooIndex.bucketBytes = bucketSize * cellBytes

	hostMemoryManager.ReportUnmanagedSpaceUsageChange(int64(cuckooIndex.allocatedBytes()))
	cuckooIndex.allocate()
	cuckooIndex.maxTrials = cuckooIndex.getMaxTrials()

	cuckooIndex.generateRandomSeeds()
	return cuckooIndex
}

// Destruct frees all allocated memory
func (c *CuckooIndex) Destruct() {
	c.transferLock.Lock()
	defer c.transferLock.Unlock()
	bytes := c.allocatedBytes()
	memutils.HostFree(c.buckets)
	c.buckets = nil
	c.hostMemoryManager.ReportUnmanagedSpaceUsageChange(-int64(bytes))
}

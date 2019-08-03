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
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"sync"
	"unsafe"
)

// archiveVectorParty is the implementation of ArchiveVectorParty
type archiveVectorParty struct {
	cVectorParty
	common.Pinnable
}

// Prune judges column mode first and sets the mode to vector party.
// Afterwards it purges unnecessary vectors based on the column mode.
func (vp *archiveVectorParty) Prune() {
	mode := vp.JudgeMode()
	switch mode {
	case common.AllValuesDefault:
		vp.values.SafeDestruct()
		vp.values = nil
		vp.counts.SafeDestruct()
		vp.counts = nil
		fallthrough
	case common.AllValuesPresent:
		vp.nulls.SafeDestruct()
		vp.nulls = nil
	}
	vp.columnMode = mode
}

// GetCount implements GetCount interface function in archiveVectorParty.
func (vp *archiveVectorParty) GetCount(offset int) uint32 {
	return *(*uint32)(vp.counts.GetValue(offset + 1))
}

// SetCount implements SetCount interface function in archiveVectorParty.
func (vp *archiveVectorParty) SetCount(offset int, count uint32) {
	vp.counts.SetValue(offset+1, unsafe.Pointer(&count))
}

// CopyOnWrite clone vector party for updates
// Only work for uncompressed archive vector party, Mode 3 vector party (has count) cannot be cloned for write
func (vp *archiveVectorParty) CopyOnWrite(batchSize int) common.ArchiveVectorParty {
	if vp.GetMode() == common.HasCountVector {
		utils.GetLogger().Panic("Mode 3 vector party should not be cloned for write.")
	}

	// archive vector party should always have allUsersDone initialized correctly with batch rwlock
	newVP := newArchiveVectorParty(batchSize, vp.dataType, vp.defaultValue, vp.AllUsersDone.L)
	newVP.Allocate(false)
	newVP.nonDefaultValueCount = vp.nonDefaultValueCount

	if vp.GetMode() == common.AllValuesDefault {
		newVP.fillWithDefaultValue()
	} else {
		if vp.values != nil {
			utils.MemCopy(newVP.values.Buffer(), vp.values.Buffer(), vp.values.Bytes)
		}

		if vp.nulls != nil {
			utils.MemCopy(newVP.nulls.Buffer(), vp.nulls.Buffer(), vp.nulls.Bytes)
		} else if vp.values != nil {
			// All values present, we need to set all bits to 1.
			newVP.nulls.SetAllValid()
		}
	}

	return newVP
}

// LoadFromDisk load archive vector party from disk
// caller should lock archive batch before using
func (vp *archiveVectorParty) LoadFromDisk(hostMemManager common.HostMemoryManager, diskStore diskstore.DiskStore, table string, shardID int, columnID, batchID int, batchVersion uint32, seqNum uint32) {
	vp.Loader.Add(1)
	go func() {
		serializer := common.NewVectorPartyArchiveSerializer(hostMemManager, diskStore, table, shardID, columnID, batchID, batchVersion, seqNum)
		err := serializer.ReadVectorParty(vp)
		if err != nil {
			utils.GetLogger().Panic(err)
		}
		vp.Loader.Done()
	}()
}

// newArchiveVectorParty creates a archive store vector party,
// archiveVectorParty use c allocated memory
func newArchiveVectorParty(length int, dataType common.DataType, defaultValue common.DataValue, locker sync.Locker) *archiveVectorParty {
	vp := &archiveVectorParty{
		cVectorParty: cVectorParty{
			baseVectorParty: baseVectorParty{
				length:       length,
				dataType:     dataType,
				defaultValue: defaultValue,
			},
		},
		Pinnable: common.Pinnable{
			AllUsersDone: sync.NewCond(locker),
		},
	}
	return vp
}

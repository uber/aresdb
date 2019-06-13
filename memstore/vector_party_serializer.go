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
)

// VectorPartyHeader is the magic header written into the beginning of each vector party file.
const VectorPartyHeader uint32 = 0xFADEFACE

// VectorPartyBaseSerializer is the base class contains basic data to read/write VectorParty
type vectorPartyBaseSerializer struct {
	shard, columnID, batchID int
	batchVersion             uint32
	seqNum                   uint32
	table                    string
	diskstore                diskstore.DiskStore
	hostMemoryManager        common.HostMemoryManager
}

// CheckVectorPartySerializable check if the archive VectorParty is serializable
func (s *vectorPartyBaseSerializer) CheckVectorPartySerializable(vp common.VectorParty) error {
	if cvp, ok := vp.(common.CVectorParty); ok {
		passed := true
		switch cvp.GetMode() {
		case common.AllValuesDefault:
			passed = vp.GetNonDefaultValueCount() == 0
		default:
			passed = vp.GetNonDefaultValueCount() > 0
		}
		if !passed {
			return utils.StackError(nil,
				"NonDefaultValueCount %d is not valid for mode %d vector with capacity %d",
				vp.GetNonDefaultValueCount(), cvp.GetMode(), vp.GetLength())
		}
	}
	return nil
}

// VectorPartyArchiveSerializer is the class to read/write archive VectorParty
type vectorPartyArchiveSerializer struct {
	vectorPartyBaseSerializer
}

// VectorPartyArchiveSerializer is the class to read/write snapshot VectorParty
type vectorPartySnapshotSerializer struct {
	vectorPartyBaseSerializer
	redoLogFile int64
	offset      uint32
}

// NewVectorPartyArchiveSerializer returns a new VectorPartySerializer
func NewVectorPartyArchiveSerializer(hostMemManager common.HostMemoryManager, diskStore diskstore.DiskStore, table string, shardID int, columnID int, batchID int, batchVersion uint32, seqNum uint32) common.VectorPartySerializer {
	return &vectorPartyArchiveSerializer{
		vectorPartyBaseSerializer{
			table:             table,
			shard:             shardID,
			columnID:          columnID,
			batchID:           batchID,
			batchVersion:      batchVersion,
			seqNum:            seqNum,
			diskstore:         diskStore,
			hostMemoryManager: hostMemManager,
		},
	}
}

// NewVectorPartySnapshotSerializer returns a new VectorPartySerializer
func NewVectorPartySnapshotSerializer(
	shard *TableShard, columnID, batchID int, batchVersion uint32, seqNum uint32, redoLogFile int64, offset uint32) common.VectorPartySerializer {
	return &vectorPartySnapshotSerializer{
		vectorPartyBaseSerializer{
			table:             shard.Schema.Schema.Name,
			shard:             shard.ShardID,
			columnID:          columnID,
			batchID:           batchID,
			batchVersion:      batchVersion,
			seqNum:            seqNum,
			diskstore:         shard.diskStore,
			hostMemoryManager: shard.HostMemoryManager,
		},
		redoLogFile,
		offset,
	}
}

// ReadVectorParty reads vector party from disk and set fields in passed-in vp.
func (s *vectorPartyArchiveSerializer) ReadVectorParty(vp common.VectorParty) error {
	if vp == nil {
		return nil
	}
	readCloser, err := s.diskstore.OpenVectorPartyFileForRead(s.table, s.columnID, s.shard,
		s.batchID, s.batchVersion, s.seqNum)
	if err != nil {
		return err
	}

	// No data on disk, return without setting fields for vp.
	if readCloser == nil {
		return nil
	}
	defer readCloser.Close()
	return vp.Read(readCloser, s)
}

// WriteVectorParty writes vector party to disk
func (s *vectorPartyArchiveSerializer) WriteVectorParty(vp common.VectorParty) error {
	if vp == nil {
		return nil
	}
	writerCloser, err := s.diskstore.OpenVectorPartyFileForWrite(s.table, s.columnID, s.shard,
		s.batchID, s.batchVersion, s.seqNum)
	if err != nil {
		return err
	}
	defer writerCloser.Close()
	return vp.Write(writerCloser)
}

// ReportVectorPartyMemoryUsage report memory usage according to underneath VectorParty property
func (s *vectorPartyArchiveSerializer) ReportVectorPartyMemoryUsage(bytes int64) {
	s.hostMemoryManager.ReportManagedObject(
		s.table, s.shard, s.batchID, s.columnID, bytes)
}

// WriteVectorParty writes snapshot vector party to disk
func (s *vectorPartySnapshotSerializer) WriteVectorParty(vp common.VectorParty) error {
	if vp == nil {
		return nil
	}
	writerCloser, err := s.diskstore.OpenSnapshotVectorPartyFileForWrite(s.table, s.shard, s.redoLogFile, s.offset, s.batchID, s.columnID)
	if err != nil {
		return err
	}
	defer writerCloser.Close()
	return vp.Write(writerCloser)
}

// ReadVectorParty reads snapshot vector party from disk
func (s *vectorPartySnapshotSerializer) ReadVectorParty(vp common.VectorParty) error {
	if vp == nil {
		return nil
	}
	readCloser, err := s.diskstore.OpenSnapshotVectorPartyFileForRead(s.table, s.shard, s.redoLogFile, s.offset, s.batchID, s.columnID)
	if err != nil {
		return err
	}

	defer readCloser.Close()
	return vp.Read(readCloser, s)
}

// CheckVectorPartySerializable check if the snapshot VectorParty is serializable, which is always true for now
func (s *vectorPartySnapshotSerializer) CheckVectorPartySerializable(vp common.VectorParty) error {
	return nil
}

// ReportVectorPartyMemoryUsage report memory usage according to underneath VectorParty property
func (s *vectorPartySnapshotSerializer) ReportVectorPartyMemoryUsage(bytes int64) {
	s.hostMemoryManager.ReportUnmanagedSpaceUsageChange(bytes)
}

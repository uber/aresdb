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

package diskstore

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/uber/aresdb/utils"
)

const data string = "data"
const redologs string = "redologs"
const snapshots string = "snapshots"
const archiveBatches string = "archiving_batches"

// Utils for data hierarchy layout.
// Following this wiki:
// https://github.com/uber/aresdb/wiki/data_disk_layout

// General path related utils for disk store.

// getPathForTableShard is used to get the directory to store a table shard given path prefix, table name and shard id.
func getPathForTableShard(prefix, table string, shardID int) string {
	tableShardDirPath := fmt.Sprintf("%s_%d", table, shardID)
	return filepath.Join(prefix, data, tableShardDirPath)
}

// Redologs Utils
// Path on disk:
//   {root_path}/data/{table_name}_{shard_id}/redologs/{creation_time}.redolog
//
// Sample:
//   /var/gForceDb/data/myTable_0/redologs/1499971253.redolog
//   /var/gForceDb/data/myTable_1/redologs/1499970221.redolog

// GetPathForTableRedologs is used to get the directory to store a table redolog given path prefix, table name and shard id.
func GetPathForTableRedologs(prefix, table string, shardID int) string {
	tableShardPath := getPathForTableShard(prefix, table, shardID)
	return filepath.Join(tableShardPath, redologs)
}

// GetPathForRedologFile is used to get on disk file path given path prefix, table name, shard id and creationTime.
func GetPathForRedologFile(prefix, table string, shardID int, creationTime int64) string {
	redologDirPath := GetPathForTableRedologs(prefix, table, shardID)
	redologName := fmt.Sprintf("%d.redolog", creationTime)
	return filepath.Join(redologDirPath, redologName)
}

// Snapshot Utils
//Path on disk:
//  {root_path}/data/{table_name}_{shard_id}/snapshots/{redlo_log}_{offset}/{batchID}/{columnID}.data
//
//Sample:
//  /var/gForceDb/data/myTable_0/snapshots/1499970253_200/-2147483648/1.data
//  /var/gForceDb/data/myTable_1/snapshots/1499970221_300/-2147483648/2.data

// GetPathForTableSnapshotDir is used to get the dir path of a snapshot given path prefix, table name and shard id.
func GetPathForTableSnapshotDir(prefix, table string, shardID int) string {
	tableShardPath := getPathForTableShard(prefix, table, shardID)
	return filepath.Join(tableShardPath, snapshots)
}

// GetPathForTableSnapshotDirPath is used to get the dir path of a snapshot given path prefix, table name, shard id
// redo log file and offset.
func GetPathForTableSnapshotDirPath(prefix, table string, shardID int, redoLogFile int64, offset uint32) string {
	tableSnapshotDirPath := GetPathForTableSnapshotDir(prefix, table, shardID)
	snapshotName := fmt.Sprintf("%d_%d", redoLogFile, offset)
	return filepath.Join(tableSnapshotDirPath, snapshotName)
}

// GetPathForTableSnapshotBatchDir is used to get the dir path of a snapshot batch given path prefix, table name,
// shard id, redo log file, offset and batchID.
func GetPathForTableSnapshotBatchDir(prefix, table string, shardID int, redoLogFile int64, offset uint32,
	batchID int) string {
	snapshotDirPath := GetPathForTableSnapshotDirPath(prefix, table, shardID, redoLogFile, offset)
	return filepath.Join(snapshotDirPath, strconv.Itoa(batchID))
}

// GetPathForTableSnapshotColumnFilePath is used to get the file path of a snapshot column given path prefix,
// table name, shard id, redo log file, offset, batchID and columnID
func GetPathForTableSnapshotColumnFilePath(prefix, table string, shardID int, redoLogFile int64, offset uint32,
	batchID, columnID int) string {
	snapshotBatchDirPath := GetPathForTableSnapshotBatchDir(prefix, table, shardID, redoLogFile, offset, batchID)
	return filepath.Join(snapshotBatchDirPath, fmt.Sprintf("%d.data", columnID))
}

// Archive batches Utils
// Path on disk:
//   {root_path}/data/{table_name}_{shard_id}/archiving_batches/{batch_id}_{batch_version}
//   {root_path}/data/{table_name}_{shard_id}/archiving_batches/{batch_id}_{batch_version}/{columnID}.data
// Note:
//   batch_id is UTC date
// 	 batch_version is the cutoff seconds in unix time.
//
// Sample:
//   /var/gForceDb/data/myTable_0/archiving_batches/2017-07-19_1499971253/1.data
//   /var/gForceDb/data/myTable_0/archiving_batches/2017-07-19_1499971253/2.data

// GetPathForTableArchiveBatchRootDir is used to get root directory path for archive batch given path prefix, table name and shard id.
func GetPathForTableArchiveBatchRootDir(prefix, table string, shardID int) string {
	tableShardPath := getPathForTableShard(prefix, table, shardID)
	return filepath.Join(tableShardPath, archiveBatches)
}

// GetPathForTableArchiveBatchDir is used to get the dir path of an archive batch version given path prefix, table name, shard id, batch id and batch version.
func GetPathForTableArchiveBatchDir(prefix, table string, shardID int, batchID string, batchVersion uint32, seqNum uint32) string {
	tableShardPath := getPathForTableShard(prefix, table, shardID)
	var batchIDAndVersionPath string
	// largest uint32 indicates there's no seqNum
	if seqNum != 0 {
		batchIDAndVersionPath = fmt.Sprintf("%s_%d-%d", batchID, batchVersion, seqNum)
	} else {
		batchIDAndVersionPath = fmt.Sprintf("%s_%d", batchID, batchVersion)
	}
	return filepath.Join(tableShardPath, archiveBatches, batchIDAndVersionPath)
}

// GetPathForTableArchiveBatchColumnFile is used to get the file path of a column inside an archive batch version given path prefix, table name, shard id, batch id, batch version and column id.
func GetPathForTableArchiveBatchColumnFile(prefix, table string, shardID int, batchID string, batchVersion uint32, seqNum uint32, columnID int) string {
	tableArchiveBatchDir := GetPathForTableArchiveBatchDir(prefix, table, shardID, batchID, batchVersion, seqNum)
	columnFileName := fmt.Sprintf("%d.data", columnID)
	return filepath.Join(tableArchiveBatchDir, columnFileName)
}

// ParseBatchIDAndVersionName will parse a batchIDAndVersion into batchID and batchVersion+seqNum.
func ParseBatchIDAndVersionName(batchIDAndVersion string) (string, uint32, uint32, error) {
	var batchID string
	var batchVersion uint64
	var seqNum uint64
	var err error

	splits := strings.Split(batchIDAndVersion, "_")
	if len(splits) == 2 {
		batchID = splits[0]

		if !strings.Contains(splits[1], "-") {
			batchVersion, err = strconv.ParseUint(splits[1], 10, 64)
		} else {
			versionSeqStr := strings.Split(splits[1], "-")
			seqNum, err = strconv.ParseUint(versionSeqStr[1], 10, 64)
			if err != nil {
				return "", 0, 0, utils.StackError(err, "Failed to parse batch seqNum: %s", batchIDAndVersion)
			}
			batchVersion, err = strconv.ParseUint(versionSeqStr[0], 10, 64)
		}
		if err != nil {
			return "", 0, 0, utils.StackError(nil, "Failed to parsed batch version: %s", batchIDAndVersion)
		}

		return batchID, uint32(batchVersion), uint32(seqNum), nil
	}
	return "", 0, 0, utils.StackError(nil, "Failed to parsed batch ID and version: %s", batchIDAndVersion)
}

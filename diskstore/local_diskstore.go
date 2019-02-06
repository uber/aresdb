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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/utils"
)

// LocalDiskStore is the implementation of Diskstore for local disk.
type LocalDiskStore struct {
	rootPath        string
	diskStoreConfig common.DiskStoreConfig
}

// NewLocalDiskStore is used to init a LocalDiskStore with rootPath.
func NewLocalDiskStore(rootPath string) DiskStore {
	return LocalDiskStore{
		rootPath:        rootPath,
		diskStoreConfig: utils.GetConfig().DiskStore,
	}
}

const timeFormatForBatchID = "2006-01-02"

// Table shard level operation

// DeleteTableShard : Completely wipe out a table shard.
func (l LocalDiskStore) DeleteTableShard(table string, shard int) error {
	tableRedologDir := getPathForTableShard(l.rootPath, table, shard)
	return os.RemoveAll(tableRedologDir)
}

// Redo Logs

// ListLogFiles : Returns the file creation unix time in second for each log file as a sorted slice.
func (l LocalDiskStore) ListLogFiles(table string, shard int) (creationUnixTime []int64, err error) {
	tableRedologDir := GetPathForTableRedologs(l.rootPath, table, shard)
	redologsFiles, err := ioutil.ReadDir(tableRedologDir)
	// The redo log directory won't get created until the first append call.
	if os.IsNotExist(err) {
		err = nil
		return
	}
	if err != nil {
		err = utils.StackError(err, "Failed to list redolog file for redolog dir: %s", tableRedologDir)
		return
	}
	for _, f := range redologsFiles {
		matchedRedologFilePattern, _ := regexp.MatchString("([0-9]+).redolog", f.Name())
		if matchedRedologFilePattern {
			creationTime, err := strconv.ParseInt(strings.Split(f.Name(), ".")[0], 10, 64)
			if err != nil {
				// Failed to parse redolog file, will continue
				utils.GetLogger().Debugf("Failed to parse redolog file: %s, will continue", f.Name())
				continue
			}
			creationUnixTime = append(creationUnixTime, creationTime)
		}
	}
	sort.Sort(utils.Int64Array(creationUnixTime))
	return creationUnixTime, nil
}

// OpenLogFileForReplay : Opens the specified log file for replay.
func (l LocalDiskStore) OpenLogFileForReplay(table string, shard int,
	creationTime int64) (utils.ReaderSeekerCloser, error) {
	logFilePath := GetPathForRedologFile(l.rootPath, table, shard, creationTime)
	f, err := os.OpenFile(logFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, utils.StackError(err, "Failed to open redolog file: %s for replay", logFilePath)
	}
	return f, nil
}

// OpenLogFileForAppend : Opens/creates the specified log file for append.
func (l LocalDiskStore) OpenLogFileForAppend(table string, shard int, creationTime int64) (io.WriteCloser, error) {
	tableRedologDir := GetPathForTableRedologs(l.rootPath, table, shard)
	if err := os.MkdirAll(tableRedologDir, 0755); err != nil {
		return nil, utils.StackError(err, "Failed to make dirs for path: %s", tableRedologDir)
	}
	logFilePath := GetPathForRedologFile(l.rootPath, table, shard, creationTime)
	mode := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	if l.diskStoreConfig.WriteSync {
		mode |= os.O_SYNC
	}
	f, err := os.OpenFile(logFilePath, mode, 0644)
	if err != nil {
		return nil, utils.StackError(err, "Failed to open redolog file: %s for append", logFilePath)
	}
	return f, nil
}

// DeleteLogFile is used to delete a specified redolog.
func (l LocalDiskStore) DeleteLogFile(table string, shard int, creationTime int64) error {
	redologFilePath := GetPathForRedologFile(l.rootPath, table, shard, creationTime)
	err := os.Remove(redologFilePath)
	if err != nil {
		return utils.StackError(err, "Failed to delete redolog file: %s", redologFilePath)
	}
	return nil
}

// TruncateLogFile is used to truncate redolog to drop the last incomplete/corrupted upsert batch.
func (l LocalDiskStore) TruncateLogFile(table string, shard int, creationTime int64, offset int64) error {
	redologFilePath := GetPathForRedologFile(l.rootPath, table, shard, creationTime)
	return os.Truncate(redologFilePath, offset)
}

// Snapshot files.

// ListSnapshotBatches : Returns the batch directories at the specified version.
func (l LocalDiskStore) ListSnapshotBatches(table string, shard int,
	redoLogFile int64, offset uint32) (batches []int, err error) {
	snapshotPath := GetPathForTableSnapshotDirPath(l.rootPath, table, shard, redoLogFile, offset)
	batchDirs, err := ioutil.ReadDir(snapshotPath)
	// No batches for this snapshot
	if os.IsNotExist(err) {
		err = nil
		return
	}
	if err != nil {
		err = utils.StackError(err, "Failed to list batch dirs for snapshot dir: %s", snapshotPath)
		return
	}
	for _, f := range batchDirs {
		batch, err := strconv.ParseInt(f.Name(), 10, 32)
		if err != nil {
			return nil, utils.StackError(err, "Failed to parse dir name: %s as valid snapshot batch dir",
				f.Name())
		}
		batches = append(batches, int(batch))
	}
	sort.Ints(batches)
	return batches, nil
}

// ListSnapshotVectorPartyFiles : Returns the vector party files under specific batch directory.
func (l LocalDiskStore) ListSnapshotVectorPartyFiles(table string, shard int,
	redoLogFile int64, offset uint32, batchID int) (columnIDs []int, err error) {
	snapshotBatchDir := GetPathForTableSnapshotBatchDir(l.rootPath, table, shard,
		redoLogFile, offset, batchID)
	vpFiles, err := ioutil.ReadDir(snapshotBatchDir)

	if os.IsNotExist(err) {
		err = nil
		return
	}

	if err != nil {
		err = utils.StackError(err, "Failed to list vp file for snapshot batch dir: %s", snapshotBatchDir)
		return
	}

	for _, f := range vpFiles {
		matchedVectorPartyFilePattern, _ := regexp.MatchString("([0-9]+).data", f.Name())
		if matchedVectorPartyFilePattern {
			var columnID int64
			columnID, err = strconv.ParseInt(strings.Split(f.Name(), ".")[0], 10, 32)
			if err != nil {
				err = utils.StackError(err, "Failed to parse file name: %s as "+
					"valid snapshot vector party file name",
					f.Name())
				return
			}
			columnIDs = append(columnIDs, int(columnID))
		} else {
			err = utils.StackError(nil, "Failed to parse file name: %s as "+
				"valid snapshot vector party file name",
				f.Name())
			return
		}
	}
	sort.Ints(columnIDs)
	return
}

// OpenSnapshotVectorPartyFileForRead : Opens the snapshot file for read at the specified version.
func (l LocalDiskStore) OpenSnapshotVectorPartyFileForRead(table string, shard int,
	redoLogFile int64, offset uint32, batchID int, columnID int) (io.ReadCloser, error) {
	snapshotFilePath := GetPathForTableSnapshotColumnFilePath(l.rootPath, table, shard, redoLogFile, offset, batchID, columnID)
	f, err := os.OpenFile(snapshotFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, utils.StackError(err, "Failed to open snapshot file: %s for read", snapshotFilePath)
	}
	return f, nil
}

// OpenSnapshotVectorPartyFileForWrite : Creates/truncates the snapshot file for write at the specified version.
func (l LocalDiskStore) OpenSnapshotVectorPartyFileForWrite(table string, shard int,
	redoLogFile int64, offset uint32, batchID int, columnID int) (io.WriteCloser, error) {
	snapshotFilePath := GetPathForTableSnapshotColumnFilePath(l.rootPath, table, shard, redoLogFile, offset, batchID, columnID)
	dir := filepath.Dir(snapshotFilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, utils.StackError(err, "Failed to make dirs for path: %s", dir)
	}
	mode := os.O_CREATE | os.O_WRONLY
	if l.diskStoreConfig.WriteSync {
		mode |= os.O_SYNC
	}
	f, err := os.OpenFile(snapshotFilePath, mode, 0644)
	os.Truncate(snapshotFilePath, 0)
	if err != nil {
		return nil, utils.StackError(err, "Failed to open snapshot file: %s for write", snapshotFilePath)
	}
	return f, nil
}

// DeleteSnapshot : Deletes snapshot directories **older than** the specified version (redolog file and offset).
func (l LocalDiskStore) DeleteSnapshot(table string, shard int, latestRedoLogFile int64, latestOffset uint32) error {
	tableSnapshotDir := GetPathForTableSnapshotDir(l.rootPath, table, shard)
	tableSnapshotFiles, err := ioutil.ReadDir(tableSnapshotDir)

	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return utils.StackError(err, "Failed to list snapshot files for snapshot dir: %s", tableSnapshotDir)
	}

	for _, f := range tableSnapshotFiles {
		matchedSnapshotDirPattern, _ := regexp.MatchString("([0-9]+)_([0-9]+)", f.Name())
		if matchedSnapshotDirPattern {
			comps := strings.Split(f.Name(), "_")
			redoLogFile, err := strconv.ParseInt(comps[0], 10, 64)
			if err != nil {
				err = nil
				// Failed to parse snapshot file name, will skip.
				utils.GetLogger().Debugf("Failed to parse latestRedoLogFile from snapshot file: %s, will continue", f.Name())
				continue
			}

			offset, err := strconv.ParseUint(comps[1], 10, 32)
			if err != nil {
				err = nil
				// Failed to parse snapshot file name, will skip.
				utils.GetLogger().Debugf("Failed to parse offset from snapshot file: %s, will continue", f.Name())
				continue
			}

			if redoLogFile < latestRedoLogFile || (redoLogFile == latestRedoLogFile && uint32(offset) < latestOffset) {
				snapshotToDeleteFilePath := GetPathForTableSnapshotDirPath(l.rootPath, table, shard, redoLogFile, uint32(offset))
				utils.GetLogger().With(
					"action", "delete_snapshot",
					"redoLog", latestRedoLogFile,
					"offset", latestOffset).Infof("delete snapshot: %s", snapshotToDeleteFilePath)
				err := os.RemoveAll(snapshotToDeleteFilePath)
				if err != nil {
					return utils.StackError(err, "Failed to delete snapshot file: %s", f.Name())
				}
			}
		}
	}
	return nil
}

// Archived vector party files.

// OpenVectorPartyFileForRead : Opens the vector party file at the specified batchVersion for read.
func (l LocalDiskStore) OpenVectorPartyFileForRead(table string, columnID int, shard, batchID int, batchVersion uint32,
	seqNum uint32) (io.ReadCloser, error) {
	batchIDTimeStr := daysSinceEpochToTimeStr(batchID)
	vectorPartyFilePath := GetPathForTableArchiveBatchColumnFile(l.rootPath, table, shard, batchIDTimeStr, batchVersion,
		seqNum, columnID)
	f, err := os.OpenFile(vectorPartyFilePath, os.O_RDONLY, 0644)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, utils.StackError(err, "Failed to open vector party file: %s for read", vectorPartyFilePath)
	}
	return f, nil
}

// OpenVectorPartyFileForWrite : Creates/truncates the vector party file at the specified batchVersion for write.
func (l LocalDiskStore) OpenVectorPartyFileForWrite(table string, columnID int, shard, batchID int, batchVersion uint32,
	seqNum uint32) (io.WriteCloser, error) {
	batchIDTimeStr := daysSinceEpochToTimeStr(batchID)
	batchDir := GetPathForTableArchiveBatchDir(l.rootPath, table, shard, batchIDTimeStr, batchVersion, seqNum)
	if err := os.MkdirAll(batchDir, 0755); err != nil {
		return nil, utils.StackError(err, "Failed to make dirs for path: %s", batchDir)
	}
	vectorPartyFilePath := GetPathForTableArchiveBatchColumnFile(l.rootPath, table, shard, batchIDTimeStr, batchVersion,
		seqNum, columnID)

	mode := os.O_CREATE | os.O_WRONLY
	if l.diskStoreConfig.WriteSync {
		mode |= os.O_SYNC
	}

	f, err := os.OpenFile(vectorPartyFilePath, mode, 0644)
	if err != nil {
		return nil, utils.StackError(err, "Failed to open vector party file: %s for write", vectorPartyFilePath)
	}
	return f, nil
}

// DeleteBatchVersions deletes all old batches with the specified batchID that have version lower than or equal to
// the specified batch  version. All columns of those batches will be deleted.
func (l LocalDiskStore) DeleteBatchVersions(table string, shard, batchID int, batchVersion uint32, seqNum uint32) error {
	batchIDTimeStr := daysSinceEpochToTimeStr(batchID)
	archiveBatchRootDir := GetPathForTableArchiveBatchRootDir(l.rootPath, table, shard)
	oldBatchDirPaths, _ := filepath.Glob(filepath.Join(archiveBatchRootDir, batchIDTimeStr) + "_*")
	for _, oldBatchDirPath := range oldBatchDirPaths {
		oldBatchInfoStr := filepath.Base(oldBatchDirPath)
		_, oldBatchVersion, oldSeqNum, _ := ParseBatchIDAndVersionName(oldBatchInfoStr)
		if oldBatchVersion < batchVersion || (oldBatchVersion == batchVersion && oldSeqNum <= seqNum) {
			err := os.RemoveAll(oldBatchDirPath)
			if err != nil {
				return utils.StackError(err, "Failed to delete batch directory: %s", oldBatchDirPath)
			}
		}
	}
	return nil
}

// DeleteBatches : Deletes all batches within [batchIDStart, batchIDEnd)
func (l LocalDiskStore) DeleteBatches(table string, shard, batchIDStart, batchIDEnd int) (int, error) {
	batchIDStartTime := daysSinceEpochToTime(batchIDStart)
	batchIDEndTime := daysSinceEpochToTime(batchIDEnd)
	tableArchiveBatchRootDir := GetPathForTableArchiveBatchRootDir(l.rootPath, table, shard)
	tableArchiveBatchDirs, err := ioutil.ReadDir(tableArchiveBatchRootDir)

	if os.IsNotExist(err) {
		return 0, nil
	}

	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, utils.StackError(err, "Failed to list archive batches from table archive batch root dir: %s",
			tableArchiveBatchRootDir)
	}

	numBatches := 0
	for _, f := range tableArchiveBatchDirs {
		batchID, batchVersion, seqNum, _ := ParseBatchIDAndVersionName(f.Name())
		batchIDTime, err := time.Parse(timeFormatForBatchID, batchID)
		if err != nil {
			utils.GetLogger().Debugf("Failed to parse batchID: %s to yyyy-MM-dd format time", batchID)
			continue
		}
		batchIDTime = batchIDTime.UTC()
		if !batchIDTime.Before(batchIDStartTime) && batchIDTime.Before(batchIDEndTime) {
			archiveBatchDir := GetPathForTableArchiveBatchDir(l.rootPath, table, shard, batchID, batchVersion, seqNum)
			err := os.RemoveAll(archiveBatchDir)
			if err != nil {
				utils.GetLogger().Debugf("Failed to delete archive batch dir: %s", archiveBatchDir)
			} else {
				numBatches++
			}
		}
	}
	return numBatches, nil
}

// DeleteColumn : Deletes all batches of the specified column.
func (l LocalDiskStore) DeleteColumn(table string, columnID int, shard int) error {
	tableArchiveBatchRootDir := GetPathForTableArchiveBatchRootDir(l.rootPath, table, shard)
	tableArchiveBatchDirs, err := ioutil.ReadDir(tableArchiveBatchRootDir)

	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return utils.StackError(err, "Failed to list archive batches from table archive batch root dir: %s",
			tableArchiveBatchRootDir)
	}
	for _, f := range tableArchiveBatchDirs {
		if f.IsDir() {
			if batchID, batchVersion, seqNum, err := ParseBatchIDAndVersionName(f.Name()); err == nil {
				vectorPartyFilePath := GetPathForTableArchiveBatchColumnFile(l.rootPath, table, shard, batchID,
					batchVersion, seqNum, columnID)
				if err = os.Remove(vectorPartyFilePath); err != nil && !os.IsNotExist(err) {
					utils.GetLogger().With(
						"vectorPartyFilePath", vectorPartyFilePath,
						"err", err,
					).Warn("Failed to delete a vector party file")
					continue
				}
			}
		}
	}
	return nil
}

func daysSinceEpochToTime(daysSinceEpoch int) time.Time {
	secondsSinceEpoch := int64(daysSinceEpoch) * 86400
	return time.Unix(secondsSinceEpoch, 0).UTC()
}

func daysSinceEpochToTimeStr(daysSinceEpoch int) string {
	return daysSinceEpochToTime(daysSinceEpoch).Format(timeFormatForBatchID)
}

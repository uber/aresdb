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
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"time"
	"unsafe"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"fmt"
	"github.com/uber/aresdb/utils"
	"path/filepath"
)

var _ = ginkgo.Describe("DiskStore", func() {
	prefix := "/tmp/testDiskStoreSuite"
	table := "myTable"
	shard := 1
	numFiles := 100
	numNumbersToWrite := 10
	createdUnixTs := make([]int64, numFiles)

	ginkgo.BeforeEach(func() {
		os.Remove(prefix)
		os.MkdirAll(prefix, 0755)
	})

	ginkgo.AfterEach(func() {
		os.RemoveAll(prefix)
	})

	ginkgo.It("Test Read/Write/Delete Redolog Files for LocalDiskstore", func() {
		// Setup directory
		redologDirPath := GetPathForTableRedologs(prefix, table, shard)
		os.MkdirAll(redologDirPath, os.ModeDir|os.ModePerm)
		for i := 0; i < numFiles; i++ {
			randInt64 := int64(rand.Uint32())
			filePath := GetPathForRedologFile(prefix, table, shard, randInt64)
			createdUnixTs[i] = randInt64
			ioutil.WriteFile(filePath, nil, os.ModePerm)
		}
		l := NewLocalDiskStore(prefix)
		logsCreatedUnixTs, err := l.ListLogFiles(table, shard)
		sort.Sort(utils.Int64Array(createdUnixTs))
		Ω(err).Should(BeNil())
		Ω(logsCreatedUnixTs).Should(Equal(createdUnixTs))
		Ω(len(logsCreatedUnixTs)).Should(Equal(numFiles))
		// Delete
		for i := 0; i < numFiles; i++ {
			err := l.DeleteLogFile(table, shard, createdUnixTs[i])
			Ω(err).Should(BeNil())
			logsCreatedUnixTs, err := l.ListLogFiles(table, shard)
			Ω(err).Should(BeNil())
			Ω(len(logsCreatedUnixTs)).Should(Equal(numFiles - i - 1))
		}
		// Write
		for i := 0; i < numFiles; i++ {
			writerCloser, err := l.OpenLogFileForAppend(table, shard, createdUnixTs[i])
			Ω(writerCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())
			ts := createdUnixTs[i]
			for j := 0; j < numNumbersToWrite; j++ {
				p := (*[8]byte)(unsafe.Pointer(&ts))[:]
				Ω(len(p)).Should(Equal(8))
				n, err := writerCloser.Write(p)
				Ω(n).Should(Equal(8))
				Ω(err).Should(BeNil())
				err = writerCloser.Close()
				Ω(err).Should(BeNil())
				writerCloser, err = l.OpenLogFileForAppend(table, shard, createdUnixTs[i])
				Ω(writerCloser).ShouldNot(BeNil())
				Ω(err).Should(BeNil())
				ts++
			}
			err = writerCloser.Close()
			Ω(err).Should(BeNil())
		}
		// Read
		for i := 0; i < numFiles; i++ {
			readCloser, err := l.OpenLogFileForReplay(table, shard, createdUnixTs[i])
			Ω(readCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())

			for j := 0; j < numNumbersToWrite; j++ {
				reader := utils.NewStreamDataReader(readCloser)
				readCreatedUnixTs, err := reader.ReadUint64()
				Ω(err).Should(BeNil())
				Ω(int64(readCreatedUnixTs)).Should(Equal(createdUnixTs[i] + int64(j)))
			}

			err = readCloser.Close()
			Ω(err).Should(BeNil())
		}
		// Delete
		for i := 0; i < numFiles; i++ {
			err := l.DeleteLogFile(table, shard, createdUnixTs[i])
			Ω(err).Should(BeNil())
			logsCreatedUnixTs, err := l.ListLogFiles(table, shard)
			Ω(err).Should(BeNil())
			Ω(len(logsCreatedUnixTs)).Should(Equal(numFiles - i - 1))
		}
	})

	ginkgo.It("Test Truncating Redolog Files for LocalDiskstore", func() {
		// Setup directory
		redologDirPath := GetPathForTableRedologs(prefix, table, shard)
		os.MkdirAll(redologDirPath, os.ModeDir|os.ModePerm)
		for i := 0; i < numFiles; i++ {
			randInt64 := int64(rand.Uint32())
			filePath := GetPathForRedologFile(prefix, table, shard, randInt64)
			createdUnixTs[i] = randInt64
			ioutil.WriteFile(filePath, nil, os.ModePerm)
		}
		l := NewLocalDiskStore(prefix)
		// Write
		for i := 0; i < numFiles; i++ {
			writerCloser, err := l.OpenLogFileForAppend(table, shard, createdUnixTs[i])
			Ω(writerCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())
			ts := createdUnixTs[i]
			for j := 0; j < numNumbersToWrite; j++ {
				p := (*[8]byte)(unsafe.Pointer(&ts))[:]
				Ω(len(p)).Should(Equal(8))
				n, err := writerCloser.Write(p)
				Ω(n).Should(Equal(8))
				Ω(err).Should(BeNil())
				err = writerCloser.Close()
				Ω(err).Should(BeNil())
				writerCloser, err = l.OpenLogFileForAppend(table, shard, createdUnixTs[i])
				Ω(writerCloser).ShouldNot(BeNil())
				Ω(err).Should(BeNil())
				ts++
			}
			err = writerCloser.Close()
			Ω(err).Should(BeNil())
		}
		// Truncate
		truncateSize := int64(numNumbersToWrite * 8 / 2)
		for i := 0; i < numFiles; i++ {
			filePath := GetPathForRedologFile(prefix, table, shard, createdUnixTs[i])
			oldFileInfo, _ := os.Stat(filePath)
			err := l.TruncateLogFile(table, shard, createdUnixTs[i], truncateSize)
			Ω(err).Should(BeNil())
			newFileInfo, _ := os.Stat(filePath)
			Ω(oldFileInfo.Size()).Should(Equal(newFileInfo.Size() * 2))
		}
		// Read
		for i := 0; i < numFiles; i++ {
			readCloser, err := l.OpenLogFileForReplay(table, shard, createdUnixTs[i])
			Ω(readCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())

			for j := 0; j < numNumbersToWrite/2; j++ {
				reader := utils.NewStreamDataReader(readCloser)
				readCreatedUnixTs, err := reader.ReadUint64()
				Ω(err).Should(BeNil())
				Ω(int64(readCreatedUnixTs)).Should(Equal(createdUnixTs[i] + int64(j)))
			}

			err = readCloser.Close()
			Ω(err).Should(BeNil())
		}
	})

	ginkgo.It("works with non-existing redolog file directory", func() {
		l := NewLocalDiskStore(prefix)
		files, err := l.ListLogFiles(table, shard)
		Ω(err).Should(BeNil())
		Ω(files).Should(BeNil())
	})

	ginkgo.It("Test List Snapshot Dir for LocalDiskstore", func() {
		// Setup directory
		snapshotDirPath := GetPathForTableSnapshotDir(prefix, table, shard)
		os.MkdirAll(snapshotDirPath, 0755)

		var redoLogFile int64 = 1
		var offset uint32 = 1
		randomBatches := make([]int, numFiles)
		for i := 0; i < numFiles; i++ {
			randomBatch := int(rand.Int31())
			filePath := GetPathForTableSnapshotColumnFilePath(prefix, table, shard, redoLogFile, offset,
				randomBatch, 0)
			os.MkdirAll(filepath.Dir(filePath), 0755)
			randomBatches[i] = randomBatch
			ioutil.WriteFile(filePath, []byte{}, os.ModePerm)
		}

		sort.Ints(randomBatches)
		l := NewLocalDiskStore(prefix)

		batches, err := l.ListSnapshotBatches(table, shard, redoLogFile, offset)
		Ω(err).Should(BeNil())
		Ω(batches).Should(Equal(randomBatches))

		batches, err = l.ListSnapshotBatches(table, shard, redoLogFile, offset+1)
		Ω(err).Should(BeNil())
		Ω(batches).Should(BeEmpty())
	})

	ginkgo.It("Test List Snapshot VP Files", func() {
		// Setup directory
		var redoLogFile int64 = 1
		var offset uint32 = 1
		batchID := 1
		batchDir := GetPathForTableSnapshotBatchDir(prefix, table, shard, redoLogFile, offset, batchID)
		os.MkdirAll(batchDir, 0755)

		randomColumns := make([]int, numFiles)
		for i := 0; i < numFiles; i++ {
			randomColumn := int(rand.Int31())
			filePath := GetPathForTableSnapshotColumnFilePath(prefix, table, shard, redoLogFile, offset,
				batchID, randomColumn)
			randomColumns[i] = randomColumn
			ioutil.WriteFile(filePath, []byte{}, os.ModePerm)
		}

		sort.Ints(randomColumns)
		l := NewLocalDiskStore(prefix)

		columns, err := l.ListSnapshotVectorPartyFiles(table, shard, redoLogFile, offset, batchID)
		Ω(err).Should(BeNil())
		Ω(columns).Should(Equal(randomColumns))

		columns, err = l.ListSnapshotVectorPartyFiles(table, shard, redoLogFile, offset, batchID+1)
		Ω(err).Should(BeNil())
		Ω(columns).Should(BeEmpty())
	})

	ginkgo.It("Test Read Snapshot Files for LocalDiskstore", func() {
		// Setup directory
		var redoLogFile int64 = 1
		var offset uint32 = 1
		batchID := 1
		batchDir := GetPathForTableSnapshotBatchDir(prefix, table, shard, redoLogFile, offset, batchID)
		os.MkdirAll(batchDir, 0755)

		randomThingToWrite := []byte("Test Read Snapshot Files for LocalDiskstore")
		randomColumns := make([]int, numFiles)
		for i := 0; i < numFiles; i++ {
			randomColumn := int(rand.Int31())
			filePath := GetPathForTableSnapshotColumnFilePath(prefix, table, shard, redoLogFile, offset, batchID, randomColumn)
			randomColumns[i] = randomColumn
			ioutil.WriteFile(filePath, randomThingToWrite, os.ModePerm)
		}
		l := NewLocalDiskStore(prefix)

		// Read
		for _, column := range randomColumns {
			readCloser, err := l.OpenSnapshotVectorPartyFileForRead(table, shard, redoLogFile, offset, batchID, column)
			Ω(readCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())
			p := make([]byte, len(randomThingToWrite))
			numBytesRead, err := readCloser.Read(p)
			Ω(numBytesRead).Should(Equal(len(randomThingToWrite)))
			Ω(p).Should(Equal(randomThingToWrite))
			err = readCloser.Close()
			Ω(err).Should(BeNil())
		}
	})

	ginkgo.It("Test Write Snapshot Files for LocalDiskstore", func() {
		// Setup directory
		var redoLogFile int64 = 1
		var offset uint32 = 1
		batchID := 1
		batchDir := GetPathForTableSnapshotBatchDir(prefix, table, shard, redoLogFile, offset, batchID)
		os.MkdirAll(batchDir, 0755)

		randomThingToWrite := []byte("Test Write Snapshot Files for LocalDiskstore")
		randomColumns := make([]int, numFiles)
		l := NewLocalDiskStore(prefix)
		// Initial set and write something longer string.
		for i := 0; i < numFiles; i++ {
			randomColumn := int(rand.Int31())
			randomColumns[i] = randomColumn
			writeCloser, err := l.OpenSnapshotVectorPartyFileForWrite(table, shard, redoLogFile, offset, batchID, randomColumn)
			Ω(writeCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())
			numBytesWritten, err := writeCloser.Write(randomThingToWrite)
			Ω(numBytesWritten).Should(Equal(len(randomThingToWrite)))
			Ω(err).Should(BeNil())
			err = writeCloser.Close()
			Ω(err).Should(BeNil())
		}
		// Read
		for _, column := range randomColumns {
			readCloser, err := l.OpenSnapshotVectorPartyFileForRead(table, shard, redoLogFile, offset, batchID, column)
			Ω(readCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())
			p := make([]byte, len(randomThingToWrite))
			numBytesRead, err := readCloser.Read(p)
			Ω(numBytesRead).Should(Equal(len(randomThingToWrite)))
			Ω(p).Should(Equal(randomThingToWrite))
			err = readCloser.Close()
			Ω(err).Should(BeNil())
		}

		anotherRandomThingToWrite := []byte("Write Snapshot Files for LocalDiskstore")
		// Should overwrite exising files with shorter string.
		for _, column := range randomColumns {
			writeCloser, err := l.OpenSnapshotVectorPartyFileForWrite(table, shard, redoLogFile, offset, batchID, column)
			Ω(writeCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())
			numBytesWritten, err := writeCloser.Write(anotherRandomThingToWrite)
			Ω(numBytesWritten).Should(Equal(len(anotherRandomThingToWrite)))
			Ω(err).Should(BeNil())
			err = writeCloser.Close()
			Ω(err).Should(BeNil())
		}

		// Read
		for _, column := range randomColumns {
			readCloser, err := l.OpenSnapshotVectorPartyFileForRead(table, shard, redoLogFile, offset, batchID, column)
			Ω(readCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())
			p := make([]byte, len(anotherRandomThingToWrite))
			numBytesRead, err := readCloser.Read(p)
			Ω(numBytesRead).Should(Equal(len(anotherRandomThingToWrite)))
			Ω(p).Should(Equal(anotherRandomThingToWrite))
			err = readCloser.Close()
			Ω(err).Should(BeNil())
		}
	})

	ginkgo.It("Test Delete Snapshot Files for LocalDiskstore", func() {
		// Setup directory
		snapshotDirPath := GetPathForTableSnapshotDir(prefix, table, shard)
		os.MkdirAll(snapshotDirPath, 0755)

		randomThingToWrite := []byte("Another Test Write Snapshot Files for LocalDiskstore")
		randomRedologFiles := make([]int64, numFiles)
		randomOffsets := make([]uint32, numFiles)
		l := NewLocalDiskStore(prefix)

		var redoLogFileLimit int64 = 3
		var offsetLimit uint32 = 10

		keptFilesMap := make(map[string]string)

		redoLogFile := redoLogFileLimit / 2
		offset := offsetLimit / 2

		// Initial set and write something longer string.
		for i := 0; i < numFiles; i++ {
			randomRedoLogFile := rand.Int63n(redoLogFileLimit)
			randomOffset := uint32(rand.Int31n(int32(offsetLimit)))
			randomRedologFiles[i] = randomRedoLogFile
			randomOffsets[i] = randomOffset
			if randomRedoLogFile > redoLogFile || (randomRedoLogFile == redoLogFile && randomOffset >= offset) {
				fileName := fmt.Sprintf("%d_%d", randomRedoLogFile, randomOffset)
				keptFilesMap[fileName] = fileName
			}
			writeCloser, err := l.OpenSnapshotVectorPartyFileForWrite(table, shard,
				randomRedoLogFile, randomOffset, 0, 0)
			Ω(writeCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())
			numBytesWritten, err := writeCloser.Write(randomThingToWrite)
			Ω(numBytesWritten).Should(Equal(len(randomThingToWrite)))
			Ω(err).Should(BeNil())
			err = writeCloser.Close()
			Ω(err).Should(BeNil())
		}

		var keptFiles []string

		for _, f := range keptFilesMap {
			keptFiles = append(keptFiles, f)
		}

		sort.Strings(keptFiles)

		err := l.DeleteSnapshot(table, shard, redoLogFile, offset)
		Ω(err).Should(BeNil())
		snapshotFileInfos, err := ioutil.ReadDir(snapshotDirPath)
		Ω(err).Should(BeNil())
		snapshotFiles := make([]string, len(snapshotFileInfos))

		for i, fileInfo := range snapshotFileInfos {
			snapshotFiles[i] = fileInfo.Name()
		}

		Ω(snapshotFiles).Should(Equal(keptFiles))
	})

	ginkgo.It("Test Read/Write Archiving Column and DeleteBatchVersions for LocalDiskstore", func() {
		l := NewLocalDiskStore(prefix)
		// Setup directory
		batchID := "1988-06-17"
		batchIDSinceEpoch := 6742
		columnID := 617

		randomThingToWrite := []byte("Test Read Snapshot Files for LocalDiskstore")
		randBatchVersions := make([]uint32, numFiles*2)

		// Write to columns
		for i := 0; i < numFiles; i++ {
			randBatchVersion := rand.Uint32()
			randBatchVersions[i] = randBatchVersion
			batchDirPath := GetPathForTableArchiveBatchDir(prefix, table, shard, batchID, randBatchVersion, 0)
			os.MkdirAll(batchDirPath, 0755)
			batchDirPath = GetPathForTableArchiveBatchDir(prefix, table, shard, batchID, randBatchVersion, 1)
			os.MkdirAll(batchDirPath, 0755)
			writeCloser, err := l.OpenVectorPartyFileForWrite(table, columnID, shard, batchIDSinceEpoch, randBatchVersion, 1)
			numBytesWritten, err := writeCloser.Write(randomThingToWrite)
			Ω(numBytesWritten).Should(Equal(len(randomThingToWrite)))
			Ω(err).Should(BeNil())
			err = writeCloser.Close()
			Ω(err).Should(BeNil())
		}

		var maxVersion uint32
		for _, version := range randBatchVersions {
			if version > maxVersion {
				maxVersion = version
			}
		}

		// Read from columns
		for i := 0; i < numFiles; i++ {
			readCloser, err := l.OpenVectorPartyFileForRead(table, columnID, shard, batchIDSinceEpoch, randBatchVersions[i], 1)
			Ω(readCloser).ShouldNot(BeNil())
			Ω(err).Should(BeNil())
			p := make([]byte, len(randomThingToWrite))
			numBytesRead, err := readCloser.Read(p)
			Ω(numBytesRead).Should(Equal(len(randomThingToWrite)))
			Ω(p).Should(Equal(randomThingToWrite))
			err = readCloser.Close()
			Ω(err).Should(BeNil())
		}

		// DeleteBatchVersions using the max version will delete all batch versions.
		batchRootDirPath := GetPathForTableArchiveBatchRootDir(prefix, table, shard)
		dirs, err := ioutil.ReadDir(batchRootDirPath)
		Ω(err).Should(BeNil())
		Ω(len(dirs)).Should(Equal(numFiles * 2))
		err = l.DeleteBatchVersions(table, shard, batchIDSinceEpoch, maxVersion, 1)
		Ω(err).Should(BeNil())
		dirs, err = ioutil.ReadDir(batchRootDirPath)
		Ω(err).Should(BeNil())
		Ω(len(dirs)).Should(Equal(0))
	})

	ginkgo.It("Test DeleteBatches with batchIDCutoff for LocalDiskstore", func() {
		l := NewLocalDiskStore(prefix)
		// Setup directory

		startBatchID := "2017-06-17"
		startBatchIDTime, err := time.Parse(timeFormatForBatchID, startBatchID)
		Ω(err).Should(BeNil())
		batchVersion := rand.Uint32()
		archiveBatchRootDirPath := GetPathForTableArchiveBatchRootDir(prefix, table, shard)

		// Create BatchID directories
		for i := 0; i < numFiles; i++ {
			batchIDTime := startBatchIDTime.Add(time.Duration(24*i) * time.Hour)
			batchID := batchIDTime.Format(timeFormatForBatchID)
			versionedBatchDirPath := GetPathForTableArchiveBatchDir(prefix, table, shard, batchID, batchVersion, 0)
			os.MkdirAll(versionedBatchDirPath, 0755)
		}

		// DeleteBatch with batchIDCutoff
		for i := 0; i < numFiles; i++ {
			batchIDTime := startBatchIDTime.Add(time.Duration(24*i) * time.Hour)
			_, err = l.DeleteBatches(table, shard, 0, int(batchIDTime.Unix()/86400))
			Ω(err).Should(BeNil())
			batchDirs, err := ioutil.ReadDir(archiveBatchRootDirPath)
			Ω(err).Should(BeNil())
			Ω(len(batchDirs)).Should(Equal(numFiles - i))
		}
		batchDirs, err := ioutil.ReadDir(archiveBatchRootDirPath)
		Ω(err).Should(BeNil())
		Ω(len(batchDirs)).Should(Equal(1))

		// Create BatchID directories
		for i := 0; i < numFiles; i++ {
			batchIDTime := startBatchIDTime.Add(time.Duration(24*i) * time.Hour)
			batchID := batchIDTime.Format(timeFormatForBatchID)
			versionedBatchDirPath := GetPathForTableArchiveBatchDir(prefix, table, shard, batchID, batchVersion, 0)
			os.MkdirAll(versionedBatchDirPath, 0755)
		}

		// DeleteBatch with batchIDCutoff
		for i := 0; i < numFiles; i += 2 {
			batchIDTime := startBatchIDTime.Add(time.Duration(24*i) * time.Hour)
			_, err = l.DeleteBatches(table, shard, 0, int(batchIDTime.Unix()/86400))
			Ω(err).Should(BeNil())
			batchDirs, err := ioutil.ReadDir(archiveBatchRootDirPath)
			Ω(err).Should(BeNil())
			Ω(len(batchDirs)).Should(Equal(numFiles - i))
		}
		batchDirs, err = ioutil.ReadDir(archiveBatchRootDirPath)
		Ω(err).Should(BeNil())
		Ω(len(batchDirs)).Should(Equal(2))

	})

	ginkgo.It("Test DeleteColumn for LocalDiskstore", func() {
		l := NewLocalDiskStore(prefix)
		// Setup directory
		numSubDir := 10
		randomThingToWrite := []byte("Test Read Snapshot Files for LocalDiskstore")
		startBatchID := "2017-06-17"
		startBatchIDTime, err := time.Parse(timeFormatForBatchID, startBatchID)
		Ω(err).Should(BeNil())
		startBatchVersion := rand.Uint32()
		archiveBatchRootDirPath := GetPathForTableArchiveBatchRootDir(prefix, table, shard)

		// Create $numSubDir batches, each batch has $numSubDir versions, each version has $numSubDir columns.
		for i := 0; i < numSubDir; i++ {
			batchIDTime := startBatchIDTime.Add(time.Duration(24*i) * time.Hour)
			batchID := batchIDTime.Format(timeFormatForBatchID)
			for j := 0; j < numSubDir; j++ {
				batchVersion := startBatchVersion + uint32(j)
				versionedBatchDirPath := GetPathForTableArchiveBatchDir(prefix, table, shard, batchID, batchVersion, 0)
				os.MkdirAll(versionedBatchDirPath, 0755)
				for k := 0; k < numSubDir; k++ {
					columnID := k
					writeCloser, err := l.OpenVectorPartyFileForWrite(table, columnID, shard, int(batchIDTime.Unix()/86400), batchVersion, 0)
					numBytesWritten, err := writeCloser.Write(randomThingToWrite)
					Ω(numBytesWritten).Should(Equal(len(randomThingToWrite)))
					Ω(err).Should(BeNil())
					err = writeCloser.Close()
					Ω(err).Should(BeNil())
				}
			}
		}

		// DeleteBatch with batchIDCutoff
		for i := 0; i < numSubDir; i++ {
			columnID := i
			l.DeleteColumn(table, columnID, shard)
			batchDirs, err := ioutil.ReadDir(archiveBatchRootDirPath)
			Ω(err).Should(BeNil())
			for _, f := range batchDirs {
				batchID, versionID, seqNum, _ := ParseBatchIDAndVersionName(f.Name())
				versionedBatchDir := GetPathForTableArchiveBatchDir(prefix, table, shard, batchID, versionID, seqNum)
				columnFiles, err := ioutil.ReadDir(versionedBatchDir)
				Ω(err).Should(BeNil())
				Ω(len(columnFiles)).Should(Equal(10 - i - 1))
				columnFilePath := GetPathForTableArchiveBatchColumnFile(prefix, table, shard, batchID, versionID, seqNum, columnID)
				_, err = os.Stat(columnFilePath)
				Ω(err).ShouldNot(BeNil())
			}
		}
	})
})

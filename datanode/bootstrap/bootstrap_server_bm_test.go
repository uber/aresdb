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
//

package bootstrap

import (
	"context"
	"fmt"
	pb "github.com/uber/aresdb/datanode/generated/proto/rpc"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/uber/aresdb/utils"
)

func TestFileTransfer(t *testing.T) {
	testFile := "/tmp/file32k"
	bigBuff := make([]byte, 32*1024)
	ioutil.WriteFile(testFile, bigBuff, 0666)

	metaStore := &metaMocks.MetaStore{}
	diskStore := &diskMocks.DiskStore{}
	grpcServer := grpc.NewServer()
	peerServer := NewPeerDataNodeServer(metaStore, diskStore)
	pb.RegisterPeerDataNodeServer(grpcServer, peerServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 0))
	if err != nil {
		t.Error(err)
	}
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	err = fileTransfer("testFileTransfer", lis.Addr().String(), testFile, 0, 0, 32*1024)
	if err != nil {
		t.Error(err)
	}

	os.Remove(testFile)
}

func fileTransfer(testName string, addr string, fileName string, chunkSize, bufferSize int32, expectedSize int) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	timeStart := utils.Now()
	client := pb.NewPeerDataNodeClient(conn)
	req := &pb.BenchmarkRequest{
		File:       fileName,
		ChunkSize:  chunkSize,
		BufferSize: bufferSize,
	}
	txClient, err := client.BenchmarkFileTransfer(context.Background(), req)
	if err != nil {
		return err
	}
	var size int
	for {
		d, err := txClient.Recv()
		if err != nil {
			break
		}
		size += len(d.Chunk)
	}
	timeUsed := utils.Now().Sub(timeStart)
	fmt.Printf("%s, time used: %d ns\n", testName, timeUsed)
	if size != expectedSize {
		return fmt.Errorf("file transfer failed, size not match, size=%d, expected:%d", size, expectedSize)
	}
	return nil
}

func BenchmarkFileTransfer(b *testing.B) {
	testFile := "/tmp/file2m"
	fileSize := 2 * 1024 * 1024

	bigBuff := make([]byte, fileSize)
	ioutil.WriteFile(testFile, bigBuff, 0666)

	metaStore := &metaMocks.MetaStore{}
	diskStore := &diskMocks.DiskStore{}
	grpcServer := grpc.NewServer()
	peerServer := NewPeerDataNodeServer(metaStore, diskStore)
	pb.RegisterPeerDataNodeServer(grpcServer, peerServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 0))
	if err != nil {
		b.Error(err)
	}
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	testCases := []struct {
		name       string
		chunkSize  int32
		bufferSize int32
	}{
		{"test chunk: 1024, buffer: 0", 1024, 0},
		{"test chunk: 2048, buffer: 0", 2048, 0},
		{"test chunk: 4096, buffer: 0", 4096, 0},
		{"test chunk: 8192, buffer: 0", 8192, 0},
		{"test chunk: 16384, buffer: 0", 16384, 0},
		{"test chunk: 32768, buffer: 0", 32768, 0},

		{"test chunk: 1024, buffer: 16000", 1024, 16 * 1024},
		{"test chunk: 2048, buffer: 16000", 2048, 16 * 1024},
		{"test chunk: 4096, buffer: 16000", 4096, 16 * 1024},
		{"test chunk: 8192, buffer: 16000", 8192, 16 * 1024},
		{"test chunk: 16384, buffer: 16000", 16384, 16 * 1024},
		{"test chunk: 32768, buffer: 16000", 32768, 16 * 1024},

		{"test chunk: 1024, buffer: 32000", 1024, 32 * 1024},
		{"test chunk: 2048, buffer: 32000", 2048, 32 * 1024},
		{"test chunk: 4096, buffer: 32000", 4096, 32 * 1024},
		{"test chunk: 8192, buffer: 32000", 8192, 32 * 1024},
		{"test chunk: 16384, buffer: 32000", 16384, 32 * 1024},
		{"test chunk: 32768, buffer: 32000", 32768, 32 * 1024},

		{"test chunk: 1024, buffer: 64000", 1024, 64 * 1024},
		{"test chunk: 2048, buffer: 64000", 2048, 64 * 1024},
		{"test chunk: 4096, buffer: 64000", 4096, 64 * 1024},
		{"test chunk: 8192, buffer: 64000", 8192, 64 * 1024},
		{"test chunk: 16384, buffer: 64000", 16384, 64 * 1024},
		{"test chunk: 32768, buffer: 64000", 32768, 64 * 1024},
	}

	for _, tc := range testCases {
		b.Run("test1", func(t *testing.B) {
			fileTransfer(tc.name, lis.Addr().String(), testFile, tc.chunkSize, tc.bufferSize, fileSize)
		})
	}

	os.Remove(testFile)
}

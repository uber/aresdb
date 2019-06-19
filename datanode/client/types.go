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
package client

import (
	"context"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
	queryCom "github.com/uber/aresdb/query/common"
)

// WithConnectionFn defines function with PeerDataNodeClient
type WithConnectionFn func(rpc.PeerDataNodeClient)

type Peer interface {
	BorrowConnection(fn WithConnectionFn) error
	Close()
	Host() topology.Host
}

// PeerSource represent a peer source which manages peer connections
type PeerSource interface {
	// BorrowConnection will borrow a connection and execute a user function.
	BorrowConnection(hostID string, fn WithConnectionFn) error
	Close()
}

type DataNodeQueryClient interface {
	// used for agg query
	Query(ctx context.Context, host topology.Host, query queryCom.AQLQuery, hll bool) (queryCom.AQLQueryResult, error)
	// used for non agg query, header is left out, only matrixData returned as raw bytes
	QueryRaw(ctx context.Context, host topology.Host, query queryCom.AQLQuery) ([]byte, error)
}

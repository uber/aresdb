package client

import (
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
)

// ConnectionPool
type ConnectionPool interface {
	// Open starts the connection pool connecting and health checking.
	Open()

	// ConnectionCount gets the current open connection count.
	ConnectionCount() int

	// NextClient gets the next client for use by the connection pool.
	NextClient() (rpc.PeerDataNodeClient, error)

	// Close the connection pool.
	Close()
}

// WithConnectionFn defines function with PeerDataNodeClient
type WithConnectionFn func(rpc.PeerDataNodeClient)

// Peer represent a peer for
type Peer interface {
	Host() topology.Host
	BorrowConnection(fn WithConnectionFn) error
}

// PeerSource represent a peer source
type PeerSource interface {
	// BorrowConnection will borrow a connection and execute a user function.
	BorrowConnection(hostID string, fn WithConnectionFn) error
}

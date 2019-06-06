package client

import (
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
)

// WithConnectionFn defines function with PeerDataNodeClient
type WithConnectionFn func(rpc.PeerDataNodeClient)

// PeerSource represent a peer source
type PeerSource interface {
	// BorrowConnection will borrow a connection and execute a user function.
	BorrowConnection(hostID string, fn WithConnectionFn) error
}

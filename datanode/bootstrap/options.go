package bootstrap

import (
	"time"
)

const (
	defaultBootstrapSessionTTL              = int64(5 * time.Minute)
	defaultMaxConcurrentTableShards         = 8
	defaultMaxCocurrentSessionPerTableShard = 2
)

// options implements bootstrap Options
type options struct {
	maxConcurrentTableShards          int
	maxConcurrentStreamsPerTableShard int
	bootstrapSessionTTL               int64
}

func (o *options) MaxConcurrentTableShards() int {
	return o.maxConcurrentTableShards
}

func (o *options) SetMaxConcurrentShards(numShards int) Options {
	o.maxConcurrentTableShards = numShards
	return o
}

func (o *options) MaxConcurrentStreamsPerTableShards() int {
	return o.maxConcurrentStreamsPerTableShard
}

func (o *options) SetMaxConcurrentStreamsPerTableShards(numStreams int) Options {
	o.maxConcurrentStreamsPerTableShard = numStreams
	return o
}

// BootstrapSessionTTL returns the ttl for bootstrap session
func (o *options) BootstrapSessionTTL() int64 {
	return o.bootstrapSessionTTL
}

// SetBootstrapSessionTTL sets the session ttl for bootstrap session
func (o *options) SetBootstrapSessionTTL(ttl int64) Options {
	o.bootstrapSessionTTL = ttl
	return o
}

// NewOptions returns bootstrap default options
func NewOptions() Options {
	return &options{
		bootstrapSessionTTL:               defaultBootstrapSessionTTL,
		maxConcurrentTableShards:          defaultMaxConcurrentTableShards,
		maxConcurrentStreamsPerTableShard: defaultMaxCocurrentSessionPerTableShard,
	}
}

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

package consumer

import "io"

// Message is a single message pulled off a Kafka topic.
type Message interface {
	// Key is a mutable reference to the message's key.
	Key() []byte
	// Value is a mutable reference to the message's value.
	Value() []byte
	// Topic is the topic from which the message was read.
	Topic() string
	// Partition is the ID of the partition from which the message was read.
	Partition() int32
	// Offset is the message's offset.
	Offset() int64
	// Ack the message
	Ack()
	// Nack the message
	Nack()
	// Cluster is the message's originated cluster.
	Cluster() string
}

// A Consumer allows users to read and process messages from a Kafka topic.
// Consumer processes within the same group use ZooKeeper to negotiate partition
// ownership, so each process sees a stream of messages from one or more
// partitions. Within a partition, messages are linearizable.
type Consumer interface {
	io.Closer

	// Name returns the name of this consumer group.
	Name() string
	// Topics returns the names of the topics being consumed.
	Topics() []string
	// Errors returns a channel of errors for the topic. To prevent deadlocks,
	// users must read from the error channel.
	//
	// All errors returned from this channel can be safely cast to the
	// consumer.Error interface, which allows structured access to the topic
	// name and partition number.
	Errors() <-chan error
	// Closed returns a channel that unblocks when the consumer successfully shuts
	// down.
	Closed() <-chan struct{}
	// Messages returns a channel of messages for the topic.
	//
	// If the consumer is not configured with nonzero buffer size, the Errors()
	// channel must be read in conjunction with Messages() to prevent deadlocks.
	Messages() <-chan Message
	// CommitUpTo marks this message and all previous messages in the same partition
	// as processed. The last processed offset for each partition is periodically
	// flushed to ZooKeeper; on startup, consumers begin processing after the last
	// stored offset.
	CommitUpTo(Message) error
}

package topology

import (
	"github.com/m3db/m3/src/cluster/shard"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	aresShard "github.com/uber/aresdb/datanode/shard"
)

var _ = Describe("host", func() {
	It("host", func() {
		host := NewHost("aresdb01", "localhost")
		Ω(host.ID()).Should(Equal("aresdb01"))
		Ω(host.Address()).Should(Equal("localhost"))
		Ω(host.String()).Should(Equal("Host<ID=aresdb01, Address=localhost>"))
	})

	It("hostshardset", func() {
		host := NewHost("aresdb01", "localhost")
		shards := aresShard.NewShards([]uint32{1, 2}, shard.Available)
		shardset := aresShard.NewShardSet(shards)

		hostShardSet := NewHostShardSet(host, shardset)
		Ω(hostShardSet.Host()).Should(Equal(host))
		Ω(hostShardSet.ShardSet()).Should(Equal(shardset))
	})
})

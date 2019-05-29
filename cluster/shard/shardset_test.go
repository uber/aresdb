package shard

import (
	"github.com/m3db/m3/src/cluster/shard"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("shardset", func() {
	It("shardset", func() {
		shards := NewShards([]uint32{1, 2}, shard.Available)
		shardset := NewShardSet(shards)

		allGot := shardset.All()
		Ω(allGot).Should(Equal(shards))

		allIdsGot := shardset.AllIDs()
		Ω(allIdsGot).Should(Equal([]uint32{1, 2}))

		idsGot := IDs(shards)
		Ω(idsGot).Should(Equal([]uint32{1, 2}))
	})
})

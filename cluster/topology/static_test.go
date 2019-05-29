package topology

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("static", func() {
	hosts := []struct {
		id     string
		addr   string
		shards []uint32
	}{
		{"h1", "h1:9000", []uint32{0}},
		{"h2", "h2:9000", []uint32{1}},
		{"h3", "h3:9000", []uint32{0}},
		{"h4", "h4:9000", []uint32{1}},
	}
	var hostShardSets []HostShardSet
	for _, h := range hosts {
		hostShardSets = append(hostShardSets,
			NewHostShardSet(NewHost(h.id, h.addr), newTestShardSet(h.shards)))
	}
	shardSet := newTestShardSet([]uint32{0, 1})

	It("static initializer and topology", func() {
		staticInit := NewStaticInitializer(
			NewStaticOptions().
				SetShardSet(shardSet).
				SetHostShardSets(hostShardSets).
				SetReplicas(2))
		topo, err := staticInit.Init()
		Ω(topo).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
		Ω(staticInit.TopologyIsSet()).Should(Equal(true))

		Ω(topo.Get()).ShouldNot(BeNil())
		w, err := topo.Watch()
		Ω(w).ShouldNot(BeNil())
		Ω(err).Should(BeNil())

		topo.Close()
	})
})

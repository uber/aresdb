// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

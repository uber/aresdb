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
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("health tracking dynamic topology", func() {
	ginkgo.It("should work happy path", func() {
		shardSet := newTestShardSet([]uint32{0, 1, 2})
		host1 := NewHost("1", "foo")
		host2 := NewHost("2", "foo")
		host3 := NewHost("3", "foo")
		hostShardSets := []HostShardSet{
			NewHostShardSet(host1, shardSet),
			NewHostShardSet(host2, shardSet),
			NewHostShardSet(host3, shardSet),
		}

		stopo := NewStaticTopology(NewStaticOptions().SetShardSet(shardSet).SetReplicas(2).SetHostShardSets(hostShardSets))

		timeIncrementer := &utils.TimeIncrementer{IncBySecond: 0}
		utils.SetClockImplementation(timeIncrementer.Now)

		topo := &healthTrackingDynamicTopoImpl{
			dynamicTopology:  stopo,
			hostsHealthiness: make(map[Host]*healthiness),
		}

		Ω(topo.Get().HostsLen()).Should(Equal(3))

		topo.MarkHostUnhealthy(host1)
		Ω(topo.Get().HostsLen()).Should(Equal(2))
		topo.MarkHostHealthy(host1)
		Ω(topo.Get().HostsLen()).Should(Equal(3))

		topo.MarkHostUnhealthy(host2)
		Ω(topo.Get().HostsLen()).Should(Equal(2))

		timeIncrementer = &utils.TimeIncrementer{IncBySecond: 11}
		utils.SetClockImplementation(timeIncrementer.Now)
		Ω(topo.Get().HostsLen()).Should(Equal(3))

	})
})

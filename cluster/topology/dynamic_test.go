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
	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"sync"
	"time"
)

var _ = Describe("dynamic", func() {
	It("InitNoTimeout", func() {
		ctrl := gomock.NewController(zap.NewNop().Sugar())
		opts, w := testSetup(ctrl)
		defer testFinish(ctrl, w)

		go w.run()
		topo, err := newDynamicTopology(opts)
		Ω(err).Should(BeNil())
		Ω(topo).ShouldNot(BeNil())

		topo.Close()
		// safe to close again
		topo.Close()
	})

	It("Back", func() {
		ctrl := gomock.NewController(zap.NewNop().Sugar())
		opts, w := testSetup(ctrl)
		defer testFinish(ctrl, w)

		go w.run()
		topo, err := newDynamicTopology(opts)
		Ω(err).Should(BeNil())

		mw, err := topo.Watch()
		Ω(err).Should(BeNil())
		Ω(mw.Get().HostsLen()).Should(Equal(3))

		opts, w = testSetup(ctrl)
		close(w.ch)
		topo, err = newDynamicTopology(opts)
		Ω(err).ShouldNot(BeNil())
	})

	It("Get", func() {
		ctrl := gomock.NewController(zap.NewNop().Sugar())
		opts, w := testSetup(ctrl)
		defer testFinish(ctrl, w)

		go w.run()
		topo, err := newDynamicTopology(opts)
		Ω(err).Should(BeNil())

		m := topo.Get()
		Ω(m.HostsLen()).Should(Equal(3))
	})

	It("Watch", func() {
		ctrl := gomock.NewController(zap.NewNop().Sugar())
		opts, w := testSetup(ctrl)
		defer testFinish(ctrl, w)

		go w.run()
		topo, err := newDynamicTopology(opts)
		Ω(err).Should(BeNil())

		tw, err := topo.Watch()
		Ω(err).Should(BeNil())
		<-tw.C()
		m := tw.Get()
		Ω(m.HostsLen()).Should(Equal(3))
		Ω(tw.Get().HostsLen()).Should(Equal(3))

		for range w.C() {
			Ω(tw.Get().HostsLen()).Should(Equal(3))
		}
	})

	It("GetUniqueShardsAndReplicas", func() {
		goodInstances := goodInstances()

		shards, err := validateInstances(goodInstances, true, 2, 3)
		Ω(err).Should(BeNil())
		Ω(len(shards)).Should(Equal(3))

		goodInstances[0].SetShards(nil)
		_, err = validateInstances(goodInstances, true, 2, 3)
		Ω(err).Should(Equal(errInstanceHasNoShardsAssignment))

		goodInstances[0].SetShards(shard.NewShards(
			[]shard.Shard{
				shard.NewShard(0),
				shard.NewShard(1),
				shard.NewShard(3),
			}))
		_, err = validateInstances(goodInstances, true, 2, 3)
		Ω(err).Should(Equal(errUnexpectedShard))

		// got h1: 1, h2: 1, 2, h3 0,2, missing a replica for 1
		goodInstances[0].SetShards(shard.NewShards(
			[]shard.Shard{
				shard.NewShard(1),
			}))
		_, err = validateInstances(goodInstances, true, 2, 3)
		Ω(err).Should(Equal(errNotEnoughReplicasForShard))

		goodInstances[0].SetShards(shard.NewShards(
			[]shard.Shard{
				shard.NewShard(0),
			}))
		goodInstances[1].SetShards(shard.NewShards(
			[]shard.Shard{
				shard.NewShard(2),
			}))
		_, err = validateInstances(goodInstances, true, 2, 3)
		// got h1:0, h2: 2, h3 0,2, missing 1
		Ω(err).Should(Equal(errMissingShard))
	})
})

func testSetup(ctrl *gomock.Controller) (DynamicOptions, *testWatch) {
	opts := NewDynamicOptions()

	watch := newTestWatch(ctrl, time.Millisecond, time.Millisecond, 100, 100)
	mockCSServices := services.NewMockServices(ctrl)
	mockCSServices.EXPECT().Watch(opts.ServiceID(), opts.QueryOptions()).Return(watch, nil)

	mockCSClient := client.NewMockClient(ctrl)
	mockCSClient.EXPECT().Services(gomock.Any()).Return(mockCSServices, nil)
	opts = opts.SetConfigServiceClient(mockCSClient)
	return opts, watch
}

func testFinish(ctrl *gomock.Controller, watch *testWatch) {
	watch.Lock()
	defer watch.Unlock()
	// Ensure only single writers to gomock.Controller
	ctrl.Finish()
}

type testWatch struct {
	sync.RWMutex

	ctrl                  *gomock.Controller
	data                  services.Service
	firstDelay, nextDelay time.Duration
	errAfter, closeAfter  int
	currentCalled         int
	ch                    chan struct{}
}

func newTestWatch(ctrl *gomock.Controller, firstDelay, nextDelay time.Duration, errAfter, closeAfter int) *testWatch {
	w := testWatch{ctrl: ctrl, firstDelay: firstDelay, nextDelay: nextDelay, errAfter: errAfter, closeAfter: closeAfter}
	w.ch = make(chan struct{})
	return &w
}

func (w *testWatch) run() {
	time.Sleep(w.firstDelay)
	w.update()
	for w.currentCalled < w.closeAfter {
		time.Sleep(w.nextDelay)
		w.update()
	}
	close(w.ch)
}

func (w *testWatch) update() {
	w.Lock()
	if w.currentCalled < w.errAfter {
		w.data = getMockService(w.ctrl)
	} else {
		w.data = nil
	}
	w.currentCalled++
	w.Unlock()
	w.ch <- struct{}{}
}

func (w *testWatch) Close() {}

func (w *testWatch) Get() services.Service {
	w.RLock()
	defer w.RUnlock()
	return w.data
}

func (w *testWatch) C() <-chan struct{} {
	return w.ch
}

func getMockService(ctrl *gomock.Controller) services.Service {
	mockService := services.NewMockService(ctrl)

	mockReplication := services.NewMockServiceReplication(ctrl)
	mockReplication.EXPECT().Replicas().Return(2).AnyTimes()
	mockService.EXPECT().Replication().Return(mockReplication).AnyTimes()

	mockSharding := services.NewMockServiceSharding(ctrl)
	mockSharding.EXPECT().NumShards().Return(3).AnyTimes()
	mockService.EXPECT().Sharding().Return(mockSharding).AnyTimes()

	mockService.EXPECT().Instances().Return(goodInstances()).AnyTimes()

	return mockService
}

func goodInstances() []services.ServiceInstance {
	i1 := services.NewServiceInstance().SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(0),
			shard.NewShard(1),
		})).SetInstanceID("h1").SetEndpoint("h1:9000")

	i2 := services.NewServiceInstance().SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(1),
			shard.NewShard(2),
		})).SetInstanceID("h2").SetEndpoint("h2:9000")

	i3 := services.NewServiceInstance().SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(2),
			shard.NewShard(0),
		})).SetInstanceID("h3").SetEndpoint("h3:9000")

	return []services.ServiceInstance{i1, i2, i3}
}

package datanode

import (
	"errors"
	m3Shard "github.com/m3db/m3/src/cluster/shard"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	aresShard "github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/datanode/bootstrap/mocks"
	"github.com/uber/aresdb/utils"
	"sync"
	"sync/atomic"
	"time"
)

var _ = ginkgo.Describe("bootstrap manager", func() {

	ginkgo.It("bootstrap manager should work", func() {
		mockBootstrapble := &mocks.Bootstrapable{}

		host0 := topology.NewHost("instance0", "http://host0:9374")
		host1 := topology.NewHost("instance1", "http://host1:9374")
		staticMapOption := topology.NewStaticOptions().
			SetReplicas(1).
			SetHostShardSets([]topology.HostShardSet{
				topology.NewHostShardSet(host0, aresShard.NewShardSet([]m3Shard.Shard{
					m3Shard.NewShard(0),
				})),
				topology.NewHostShardSet(host1, aresShard.NewShardSet([]m3Shard.Shard{
					m3Shard.NewShard(0),
				})),
			}).SetShardSet(aresShard.NewShardSet([]m3Shard.Shard{
			m3Shard.NewShard(0),
		}))
		staticTopology, _ := topology.NewStaticInitializer(staticMapOption).Init()
		opts := bootstrap.NewOptions()

		manager := NewBootstrapManager(
			"host1",
			mockBootstrapble,
			opts,
			staticTopology,
		)

		var timer uint32 = 0
		var triggered uint32 = 0

		// 1. test multiple triggering of manager.Bootstrap()
		utils.SetCurrentTime(time.Unix(10, 0))
		defer utils.ResetClockImplementation()

		// bootstrapble only should be triggered twice although manager is triggered 10 times
		// first time when first manager.Bootstrap() is called
		// second time when first bootstrap task is finished and manager detects pending task exists
		mockBootstrapble.On("Bootstrap", mock.Anything, "host1", staticTopology, mock.Anything, opts).Run(func(args mock.Arguments) {
			atomic.AddUint32(&triggered, 1)
			// simulating 10 seconds to finish
			for atomic.LoadUint32(&timer) < 10 {
			}
		}).Return(nil).Twice()

		mutex := &sync.Mutex{}
		errs := xerrors.NewMultiError()
		wg := &sync.WaitGroup{}

		// before run
		Ω(manager.IsBootstrapped()).Should(BeFalse())
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				// simulate time increment one sec at a time
				// by the time 10th bootstrap trigger on manager,
				// the first bootstrap task should just finish
				atomic.AddUint32(&timer, 1)
				err := manager.Bootstrap()
				if err != nil {
					mutex.Lock()
					errs = errs.Add(err)
					mutex.Unlock()
				}
				wg.Done()
			}()
		}
		wg.Wait()

		// after run
		Ω(errs.FinalError()).Should(BeNil())
		Ω(triggered).Should(Equal(uint32(2)))
		Ω(manager.IsBootstrapped()).Should(BeTrue())
		t, isBootstapped := manager.LastBootstrapCompletionTime()
		Ω(t).Should(Equal(utils.Now()))
		Ω(isBootstapped).Should(BeTrue())

		// 2. test error case
		mockBootstrapble.On("Bootstrap", mock.Anything, "host1", staticTopology, mock.Anything, opts).
			Return(errors.New("failed to run bootstrap")).Once()

		err := manager.Bootstrap()
		Ω(err).ShouldNot(BeNil())
	})

})

package datanode

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/memstore"
	memStoreMocks "github.com/uber/aresdb/memstore/mocks"
)

var _ = ginkgo.Describe("datanode", func() {

	ginkgo.It("checkShardReadiness", func() {
		mockMemStore := new(memStoreMocks.MemStore)
		dataNode := dataNode{
			memStore: mockMemStore,
		}

		t1s0 := memstore.TableShard{
			BootstrapState: bootstrap.BootstrapNotStarted,
		}
		t1s1 := memstore.TableShard{
			BootstrapState: bootstrap.Bootstrapped,
		}
		t1s2 := memstore.TableShard{
			BootstrapState: bootstrap.BootstrapNotStarted,
		}
		t2s0 := memstore.TableShard{
			BootstrapState: bootstrap.Bootstrapped,
		}
		t2s1 := memstore.TableShard{
			BootstrapState: bootstrap.Bootstrapped,
		}
		t2s2 := memstore.TableShard{
			BootstrapState: bootstrap.Bootstrapped,
		}

		mockMemStore.On("GetTableShard", "t1", 0).
			Run(func(args mock.Arguments) {
				t1s0.Users.Add(1)
			}).
			Return(&t1s0, nil).Once()

		mockMemStore.On("GetTableShard", "t1", 1).
			Run(func(args mock.Arguments) {
				t1s1.Users.Add(1)
			}).
			Return(&t1s1, nil).Once()

		mockMemStore.On("GetTableShard", "t1", 2).
			Run(func(args mock.Arguments) {
				t1s2.Users.Add(1)
			}).
			Return(&t1s2, nil).Once()

		mockMemStore.On("GetTableShard", "t2", 0).
			Run(func(args mock.Arguments) {
				t2s0.Users.Add(1)
			}).
			Return(&t2s0, nil).Once()

		mockMemStore.On("GetTableShard", "t2", 1).
			Run(func(args mock.Arguments) {
				t2s1.Users.Add(1)
			}).
			Return(&t2s1, nil).Once()

		mockMemStore.On("GetTableShard", "t2", 2).
			Run(func(args mock.Arguments) {
				t2s2.Users.Add(1)
			}).
			Return(&t2s2, nil).Once()

		shards := []uint32{0, 1, 2}
		Ω(dataNode.checkShardReadiness([]string{"t1", "t2"}, shards)).Should(Equal(1))
		Ω(shards[0]).Should(Equal(uint32(1)))
	})
})

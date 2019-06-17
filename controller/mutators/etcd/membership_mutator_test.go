package etcd

import (
	"github.com/m3db/m3/src/cluster/kv/mem"
	"testing"

	"github.com/stretchr/testify/assert"
	modelspb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/utils"
)

func TestMembershipMutator(t *testing.T) {

	instance1 := models.Instance{
		Name: "inst1",
		Host: "foo.com",
		Port: 1234,
	}

	t.Run("crud ops should work", func(t *testing.T) {
		// test setup
		etcdStore := mem.NewStore()

		_, err := etcdStore.Set(utils.InstanceListKey("ns1"), &modelspb.EntityList{})
		assert.NoError(t, err)

		// test
		membershipMutator := NewMembershipMutator(etcdStore)
		res, err := membershipMutator.GetInstances("ns1")
		assert.NoError(t, err)
		assert.Len(t, res, 0)

		err = membershipMutator.Join("ns1", instance1)
		assert.NoError(t, err)

		res, err = membershipMutator.GetInstances("ns1")
		assert.NoError(t, err)
		assert.Len(t, res, 1)

		var instanceRes models.Instance
		instanceRes, err = membershipMutator.GetInstance("ns1", instance1.Name)
		assert.NoError(t, err)
		assert.Equal(t, instance1, instanceRes)

		err = membershipMutator.Leave("ns1", instance1.Name)
		assert.NoError(t, err)

		res, err = membershipMutator.GetInstances("ns1")
		assert.NoError(t, err)
		assert.Len(t, res, 0)
	})
}

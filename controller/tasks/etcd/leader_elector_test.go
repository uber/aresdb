package etcd

import (
	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/services/leader/campaign"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLeaderElect(t *testing.T) {

	t.Run("leader elector should work", func(t *testing.T) {
		// test setup

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		leaderService := services.NewMockLeaderService(ctrl)

		leaderCh := make(chan campaign.Status, 1)
		leaderCh <- campaign.NewStatus(campaign.Leader)
		leaderService.EXPECT().Campaign("", gomock.Any()).Return(leaderCh, nil).AnyTimes()
		leaderService.EXPECT().Resign("").DoAndReturn(func(electionID string) error {
			leaderCh <- campaign.NewStatus(campaign.Follower)
			return nil
		})

		elector := NewLeaderElector(leaderService)
		err := elector.Start()
		assert.NoError(t, err)
		<- elector.C()
		assert.Equal(t, elector.Status(), Leader)

		err = elector.Resign()
		assert.NoError(t, err)
		<- elector.C()
		assert.Equal(t, elector.Status(), Follower)
	})
}
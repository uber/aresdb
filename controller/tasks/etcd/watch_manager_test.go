package etcd

import (
	"github.com/golang/mock/gomock"
	xwatch "github.com/m3db/m3x/watch"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWatchManager(t *testing.T) {

	t.Run("leader elector should work", func(t *testing.T) {
		// test setup

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		watchable := xwatch.NewWatchable()
		manager := NewWatchManager(func(namespace string) (updatable xwatch.Updatable, e error) {
			_, watch, err := watchable.Watch()
			return watch, err
		})

		err := manager.AddNamespace("ns1")
		assert.NoError(t, err)

		notify := make(chan struct{})

		go func() {
			<-manager.C()
			close(notify)
		}()

		_ = watchable.Update(nil)
		<-notify
	})
}

// +build integration

package upgraded_test

import (
	"testing"

	"github.com/spoke-d/thermionic/internal/cluster/upgraded"
	"github.com/spoke-d/thermionic/internal/node"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

// A node can unblock other nodes that were waiting for a cluster upgrade to
// complete.
func TestNewNotifyUpgradeCompleted(t *testing.T) {
	f := libtesting.NewFixtures(t)
	defer f.Cleanup()

	gateway0 := f.Bootstrap()
	gateway1 := f.Grow()

	state0 := f.State(gateway0)

	cert0 := gateway0.Cert()

	notifyTask := upgraded.New(upgradedStateShim{state: state0}, cert0, node.ConfigSchema)
	err := notifyTask.Run()
	if err != nil {
		t.Error(err)
	}

	gateway1.WaitUpgradeNotification()
}

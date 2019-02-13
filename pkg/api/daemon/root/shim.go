package root

import (
	"github.com/spoke-d/thermionic/internal/cluster/notifier"
	"github.com/spoke-d/thermionic/pkg/api"
)

type notifierStateShim struct {
	state api.State
}

func makeNotifierStateShim(state api.State) notifierStateShim {
	return notifierStateShim{
		state: state,
	}
}

func (s notifierStateShim) Cluster() notifier.Cluster {
	return s.state.Cluster()
}

func (s notifierStateShim) Node() notifier.Node {
	return s.state.Node()
}

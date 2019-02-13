package notifier_test

import (
	"github.com/spoke-d/thermionic/internal/cluster/notifier"
	"github.com/spoke-d/thermionic/internal/state"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/filesystem_mock.go github.com/spoke-d/thermionic/internal/fsys FileSystem
//go:generate mockgen -package mocks -destination mocks/node_mock.go github.com/spoke-d/thermionic/internal/cluster/notifier Node
//go:generate mockgen -package mocks -destination mocks/cluster_mock.go github.com/spoke-d/thermionic/internal/cluster/notifier Cluster
//go:generate mockgen -package mocks -destination mocks/state_mock.go github.com/spoke-d/thermionic/internal/cluster/notifier State
//go:generate mockgen -package mocks -destination mocks/server_mock.go github.com/spoke-d/thermionic/internal/cluster Server
//go:generate mockgen -package mocks -destination mocks/store_mock.go github.com/spoke-d/thermionic/internal/cluster StoreProvider
//go:generate mockgen -package mocks -destination mocks/clock_mock.go github.com/spoke-d/thermionic/internal/clock Clock

type notifyStateShim struct {
	state *state.State
}

func (s notifyStateShim) Node() notifier.Node {
	return s.state.Node()
}

func (s notifyStateShim) Cluster() notifier.Cluster {
	return s.state.Cluster()
}

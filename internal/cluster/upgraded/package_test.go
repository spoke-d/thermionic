package upgraded_test

import (
	"github.com/spoke-d/thermionic/internal/cluster/upgraded"
	"github.com/spoke-d/thermionic/internal/state"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/filesystem_mock.go github.com/spoke-d/thermionic/internal/fsys FileSystem
//go:generate mockgen -package mocks -destination mocks/node_mock.go github.com/spoke-d/thermionic/internal/cluster/upgraded Node
//go:generate mockgen -package mocks -destination mocks/cluster_mock.go github.com/spoke-d/thermionic/internal/cluster/upgraded Cluster
//go:generate mockgen -package mocks -destination mocks/state_mock.go github.com/spoke-d/thermionic/internal/cluster/upgraded State
//go:generate mockgen -package mocks -destination mocks/os_mock.go github.com/spoke-d/thermionic/internal/cluster/upgraded OS
//go:generate mockgen -package mocks -destination mocks/notifier_mock.go github.com/spoke-d/thermionic/internal/cluster/upgraded Notifier,NotifierProvider
//go:generate mockgen -package mocks -destination mocks/clock_mock.go github.com/spoke-d/thermionic/internal/clock Clock

type upgradedStateShim struct {
	state *state.State
}

func (s upgradedStateShim) Node() upgraded.Node {
	return s.state.Node()
}

func (s upgradedStateShim) Cluster() upgraded.Cluster {
	return s.state.Cluster()
}

func (s upgradedStateShim) OS() upgraded.OS {
	return s.state.OS()
}

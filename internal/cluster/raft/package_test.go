package raft_test

import (
	"fmt"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"

	rafthttp "github.com/CanonicalLtd/raft-http"
	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/db"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/filesystem_mock.go github.com/spoke-d/thermionic/internal/fsys FileSystem
//go:generate mockgen -package mocks -destination mocks/node_mock.go github.com/spoke-d/thermionic/internal/cluster/raft Node,RaftNodeMediator
//go:generate mockgen -package mocks -destination mocks/dialer_mock.go github.com/spoke-d/thermionic/internal/cluster/raft DialerProvider
//go:generate mockgen -package mocks -destination mocks/address_mock.go github.com/spoke-d/thermionic/internal/cluster/raft AddressResolver
//go:generate mockgen -package mocks -destination mocks/http_provider_mock.go github.com/spoke-d/thermionic/internal/cluster/raft HTTPProvider
//go:generate mockgen -package mocks -destination mocks/network_provider_mock.go github.com/spoke-d/thermionic/internal/cluster/raft NetworkProvider
//go:generate mockgen -package mocks -destination mocks/logs_provider_mock.go github.com/spoke-d/thermionic/internal/cluster/raft LogsProvider,LogStore
//go:generate mockgen -package mocks -destination mocks/snapshot_store_provider_mock.go github.com/spoke-d/thermionic/internal/cluster/raft SnapshotStoreProvider
//go:generate mockgen -package mocks -destination mocks/registry_provider_mock.go github.com/spoke-d/thermionic/internal/cluster/raft RegistryProvider
//go:generate mockgen -package mocks -destination mocks/raft_provider_mock.go github.com/spoke-d/thermionic/internal/cluster/raft RaftProvider
//go:generate mockgen -package mocks -destination mocks/query_mock.go github.com/spoke-d/thermionic/internal/db Query,QueryCluster,QueryNode,Transaction
//go:generate mockgen -package mocks -destination mocks/raft_mock.go github.com/hashicorp/raft Transport,SnapshotStore,FSM
//go:generate mockgen -package mocks -destination mocks/result_mock.go database/sql Result
//go:generate mockgen -package mocks -destination mocks/net_mock.go net Addr

func setup(t *testing.T, fn func(*testing.T, *gomock.Controller, *gomock.Controller)) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	t.Run("run", func(t *testing.T) {
		fn(t, ctrl, ctrl2)
	})
}

type nodeTransactionMatcher struct {
	tx  *db.NodeTx
	err error
}

func NodeTransactionMatcher(tx *db.NodeTx) *nodeTransactionMatcher {
	return &nodeTransactionMatcher{
		tx: tx,
	}
}

func (m *nodeTransactionMatcher) Matches(x interface{}) bool {
	fn, ok := x.(func(*db.NodeTx) error)
	if ok {
		m.err = fn(m.tx)
		return true
	}
	return false
}

func (m *nodeTransactionMatcher) Err() error {
	return m.err
}

func (*nodeTransactionMatcher) String() string {
	return "NodeTransaction"
}

type dialerMatcher struct {
	x rafthttp.Dial
}

func DialerMatcher(x rafthttp.Dial) gomock.Matcher {
	return dialerMatcher{x}
}
func (m dialerMatcher) Matches(x interface{}) bool {
	check := func(x, y rafthttp.Dial) bool {
		a := runtime.FuncForPC(reflect.ValueOf(x).Pointer()).Name()
		b := runtime.FuncForPC(reflect.ValueOf(y).Pointer()).Name()
		return a == b
	}
	if y, ok := x.(rafthttp.Dial); ok {
		return check(m.x, y)
	} else if y, ok := x.(func(string, time.Duration) (net.Conn, error)); ok {
		return check(m.x, y)
	}
	return false
}
func (m dialerMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

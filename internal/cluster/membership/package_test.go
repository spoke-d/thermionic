package membership_test

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	rafthttp "github.com/CanonicalLtd/raft-http"
	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/db"
	querycluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/state"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/filesystem_mock.go github.com/spoke-d/thermionic/internal/fsys FileSystem
//go:generate mockgen -package mocks -destination mocks/config_mock.go github.com/spoke-d/thermionic/internal/cluster/membership NodeConfigProvider,ClusterConfigProvider
//go:generate mockgen -package mocks -destination mocks/gateway_mock.go github.com/spoke-d/thermionic/internal/cluster/membership Gateway
//go:generate mockgen -package mocks -destination mocks/state_mock.go github.com/spoke-d/thermionic/internal/cluster/membership State
//go:generate mockgen -package mocks -destination mocks/cluster_mock.go github.com/spoke-d/thermionic/internal/cluster/membership Cluster
//go:generate mockgen -package mocks -destination mocks/raft_mock.go github.com/spoke-d/thermionic/internal/cluster/membership RaftInstance
//go:generate mockgen -package mocks -destination mocks/node_mock.go github.com/spoke-d/thermionic/internal/cluster/membership Node
//go:generate mockgen -package mocks -destination mocks/dialer_mock.go github.com/spoke-d/thermionic/internal/cluster/membership DialerProvider
//go:generate mockgen -package mocks -destination mocks/membership_mock.go github.com/spoke-d/thermionic/internal/cluster/membership Membership
//go:generate mockgen -package mocks -destination mocks/os_mock.go github.com/spoke-d/thermionic/internal/cluster/membership OS
//go:generate mockgen -package mocks -destination mocks/query_mock.go github.com/spoke-d/thermionic/internal/db Query,QueryCluster,QueryNode,Transaction
//go:generate mockgen -package mocks -destination mocks/clock_mock.go github.com/spoke-d/thermionic/internal/clock Clock
//go:generate mockgen -package mocks -destination mocks/sleeper_mock.go github.com/spoke-d/thermionic/internal/clock Sleeper
//go:generate mockgen -package mocks -destination mocks/raftmembership_mock.go github.com/CanonicalLtd/raft-membership Changer
//go:generate mockgen -package mocks -destination mocks/result_mock.go database/sql Result

func setup(t *testing.T, fn func(*testing.T, *gomock.Controller, *gomock.Controller)) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	t.Run("run", func(t *testing.T) {
		fn(t, ctrl, ctrl2)
	})
}

type clusterTransactionMatcher struct {
	tx  *db.ClusterTx
	err error
}

func ClusterTransactionMatcher(tx *db.ClusterTx) *clusterTransactionMatcher {
	return &clusterTransactionMatcher{
		tx: tx,
	}
}

func (m *clusterTransactionMatcher) Matches(x interface{}) bool {
	fn, ok := x.(func(*db.ClusterTx) error)
	if ok {
		m.err = fn(m.tx)
		return true
	}
	return false
}

func (m *clusterTransactionMatcher) Err() error {
	return m.err
}

func (*clusterTransactionMatcher) String() string {
	return "ClusterTransaction"
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

type nodeInfoDestSelectObjectsMatcher struct {
	x []db.NodeInfo
}

func NodeInfoDestSelectObjectsMatcher(v []db.NodeInfo) gomock.Matcher {
	return nodeInfoDestSelectObjectsMatcher{
		x: v,
	}
}

func (m nodeInfoDestSelectObjectsMatcher) Matches(x interface{}) bool {
	ref := reflect.ValueOf(x)
	i := 0
	for _, v := range m.x {
		values := ref.Call([]reflect.Value{
			reflect.ValueOf(i),
		})
		if num := len(values); num != 1 {
			panic(fmt.Sprintf("expected 1 values got %d", num))
		}
		slice := values[0]
		if num := slice.Len(); num != 7 {
			panic(fmt.Sprintf("expected 7 values got %d", num))
		}
		slice.Index(0).Elem().Elem().SetInt(v.ID)
		slice.Index(1).Elem().Elem().SetString(v.Name)
		slice.Index(2).Elem().Elem().SetString(v.Address)
		slice.Index(3).Elem().Elem().SetString(v.Description)
		slice.Index(4).Elem().Elem().SetInt(int64(v.Schema))
		slice.Index(5).Elem().Elem().SetInt(int64(v.APIExtensions))
		slice.Index(6).Elem().Elem().Set(reflect.ValueOf(v.Heartbeat))
		i++
	}
	return true
}

func (m nodeInfoDestSelectObjectsMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

type nodeDestSelectObjectsMatcher struct {
	x []db.RaftNode
}

func NodeDestSelectObjectsMatcher(v []db.RaftNode) gomock.Matcher {
	return nodeDestSelectObjectsMatcher{
		x: v,
	}
}

func (m nodeDestSelectObjectsMatcher) Matches(x interface{}) bool {
	ref := reflect.ValueOf(x)
	i := 0
	for _, v := range m.x {
		values := ref.Call([]reflect.Value{
			reflect.ValueOf(i),
		})
		if num := len(values); num != 1 {
			panic(fmt.Sprintf("expected 1 values got %d", num))
		}
		slice := values[0]
		if num := slice.Len(); num != 2 {
			panic(fmt.Sprintf("expected 2 values got %d", num))
		}
		slice.Index(0).Elem().Elem().SetInt(v.ID)
		slice.Index(1).Elem().Elem().SetString(v.Address)
		i++
	}
	return true
}

func (m nodeDestSelectObjectsMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

type dialMatcher struct {
	x rafthttp.Dial
}

func DialMatcher(x rafthttp.Dial) gomock.Matcher {
	return dialMatcher{x}
}

func (m dialMatcher) Matches(x interface{}) bool {
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

func (m dialMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

type operationDestSelectObjectsMatcher struct {
	x []db.Operation
}

func OperationDestSelectObjectsMatcher(v []db.Operation) gomock.Matcher {
	return operationDestSelectObjectsMatcher{
		x: v,
	}
}

func (m operationDestSelectObjectsMatcher) Matches(x interface{}) bool {
	ref := reflect.ValueOf(x)
	i := 0
	for _, v := range m.x {
		values := ref.Call([]reflect.Value{
			reflect.ValueOf(i),
		})
		if num := len(values); num != 1 {
			panic(fmt.Sprintf("expected 1 values got %d", num))
		}
		slice := values[0]
		if num := slice.Len(); num != 4 {
			panic(fmt.Sprintf("expected 4 values got %d", num))
		}
		slice.Index(0).Elem().Elem().SetInt(v.ID)
		slice.Index(1).Elem().Elem().SetString(v.UUID)
		slice.Index(2).Elem().Elem().SetString(v.NodeAddress)
		slice.Index(3).Elem().Elem().SetString(string(v.Type))
		i++
	}
	return true
}

func (m operationDestSelectObjectsMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

type membershipStateShim struct {
	state *state.State
}

func makeMembershipStateShim(state *state.State) membershipStateShim {
	return membershipStateShim{
		state: state,
	}
}

func (s membershipStateShim) Node() membership.Node {
	return s.state.Node()
}

func (s membershipStateShim) Cluster() membership.Cluster {
	return s.state.Cluster()
}

func (s membershipStateShim) OS() membership.OS {
	return s.state.OS()
}

type membershipGatewayShim struct {
	gateway *cluster.Gateway
}

func makeMembershipGatewayShim(gateway *cluster.Gateway) membershipGatewayShim {
	return membershipGatewayShim{
		gateway: gateway,
	}
}

func (s membershipGatewayShim) Init(certInfo *cert.Info) error {
	return s.gateway.Init(certInfo)
}

func (s membershipGatewayShim) Shutdown() error {
	return s.gateway.Shutdown()
}

func (s membershipGatewayShim) WaitLeadership() error {
	return s.gateway.WaitLeadership()
}

func (s membershipGatewayShim) RaftNodes() ([]db.RaftNode, error) {
	return s.gateway.RaftNodes()
}

func (s membershipGatewayShim) Raft() membership.RaftInstance {
	return s.gateway.Raft()
}

func (s membershipGatewayShim) DB() membership.Node {
	return s.gateway.DB()
}

func (s membershipGatewayShim) IsDatabaseNode() bool {
	return s.gateway.IsDatabaseNode()
}

func (s membershipGatewayShim) Cert() *cert.Info {
	return s.gateway.Cert()
}

func (s membershipGatewayShim) Reset(certInfo *cert.Info) error {
	return s.gateway.Reset(certInfo)
}

func (s membershipGatewayShim) DialFunc() dqlite.DialFunc {
	return s.gateway.DialFunc()
}

func (s membershipGatewayShim) ServerStore() querycluster.ServerStore {
	return s.gateway.ServerStore()
}

func (s membershipGatewayShim) Context() context.Context {
	return s.gateway.Context()
}

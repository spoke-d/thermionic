package services_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/golang/mock/gomock"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/filesystem_mock.go github.com/spoke-d/thermionic/internal/fsys FileSystem
//go:generate mockgen -package mocks -destination mocks/state_mock.go github.com/spoke-d/thermionic/internal/cluster/services State
//go:generate mockgen -package mocks -destination mocks/cluster_mock.go github.com/spoke-d/thermionic/internal/cluster/services Cluster
//go:generate mockgen -package mocks -destination mocks/node_mock.go github.com/spoke-d/thermionic/internal/cluster/services Node
//go:generate mockgen -package mocks -destination mocks/os_mock.go github.com/spoke-d/thermionic/internal/cluster/services OS
//go:generate mockgen -package mocks -destination mocks/query_mock.go github.com/spoke-d/thermionic/internal/db Query,QueryCluster,QueryNode,Transaction
//go:generate mockgen -package mocks -destination mocks/clock_mock.go github.com/spoke-d/thermionic/internal/clock Clock
//go:generate mockgen -package mocks -destination mocks/sleeper_mock.go github.com/spoke-d/thermionic/internal/clock Sleeper
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

type serviceNodeInfoDestSelectObjectsMatcher struct {
	x []db.ServiceNodeInfo
}

func ServiceNodeInfoDestSelectObjectsMatcher(v []db.ServiceNodeInfo) gomock.Matcher {
	return serviceNodeInfoDestSelectObjectsMatcher{
		x: v,
	}
}

func (m serviceNodeInfoDestSelectObjectsMatcher) Matches(x interface{}) bool {
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
		if num := slice.Len(); num != 6 {
			panic(fmt.Sprintf("expected 6 values got %d", num))
		}
		slice.Index(0).Elem().Elem().SetInt(v.ID)
		slice.Index(1).Elem().Elem().SetString(v.Name)
		slice.Index(2).Elem().Elem().SetString(v.Address)
		slice.Index(3).Elem().Elem().SetString(v.DaemonAddress)
		slice.Index(4).Elem().Elem().SetString(v.DaemonNonce)
		slice.Index(5).Elem().Elem().Set(reflect.ValueOf(v.Heartbeat))
		i++
	}
	return true
}

func (m serviceNodeInfoDestSelectObjectsMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

package schedules_test

import (
	"fmt"
	"reflect"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/golang/mock/gomock"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/gateway_mock.go github.com/spoke-d/thermionic/internal/schedules Gateway
//go:generate mockgen -package mocks -destination mocks/cluster_mock.go github.com/spoke-d/thermionic/internal/schedules Cluster
//go:generate mockgen -package mocks -destination mocks/node_mock.go github.com/spoke-d/thermionic/internal/schedules Node
//go:generate mockgen -package mocks -destination mocks/daemon_mock.go github.com/spoke-d/thermionic/internal/schedules Daemon
//go:generate mockgen -package mocks -destination mocks/clock_mock.go github.com/spoke-d/thermionic/internal/clock Clock
//go:generate mockgen -package mocks -destination mocks/query_mock.go github.com/spoke-d/thermionic/internal/db Query,QueryCluster,QueryNode,Transaction
//go:generate mockgen -package mocks -destination mocks/result_mock.go database/sql Result

type clusterTransactionMatcher struct {
	clusterTx *db.ClusterTx
	err       error
}

func ClusterTransactionMatcher(clusterTx *db.ClusterTx) *clusterTransactionMatcher {
	return &clusterTransactionMatcher{
		clusterTx: clusterTx,
	}
}

func (m *clusterTransactionMatcher) Matches(x interface{}) bool {
	fn, ok := x.(func(*db.ClusterTx) error)
	if ok {
		m.err = fn(m.clusterTx)
		return true
	}
	return false
}

func (m *clusterTransactionMatcher) Err() error {
	return m.err
}

func (*clusterTransactionMatcher) String() string {
	return "transaction"
}

type taskDestSelectObjectsMatcher struct {
	x []db.Task
}

func TaskDestSelectObjectsMatcher(v []db.Task) gomock.Matcher {
	return taskDestSelectObjectsMatcher{
		x: v,
	}
}

func (m taskDestSelectObjectsMatcher) Matches(x interface{}) bool {
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
		var status int64
		slice.Index(0).Elem().Elem().SetInt(v.ID)
		slice.Index(1).Elem().Elem().SetString(v.UUID)
		slice.Index(2).Elem().Elem().SetString(v.NodeAddress)
		slice.Index(3).Elem().Elem().SetString(v.Query)
		slice.Index(4).Elem().Elem().SetInt(v.Schedule)
		slice.Index(5).Elem().Elem().SetString(v.Result)
		slice.Index(6).Elem().Elem().SetInt(status)
		v.Status = int(status)
		i++
	}
	return true
}

func (m taskDestSelectObjectsMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

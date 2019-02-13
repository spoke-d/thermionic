package heartbeat_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/golang/mock/gomock"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/query_mock.go github.com/spoke-d/thermionic/internal/db Query,QueryCluster,QueryNode,Transaction
//go:generate mockgen -package mocks -destination mocks/gateway_mock.go github.com/spoke-d/thermionic/internal/cluster/heartbeat Gateway
//go:generate mockgen -package mocks -destination mocks/certconfig_mock.go github.com/spoke-d/thermionic/internal/cluster/heartbeat CertConfig
//go:generate mockgen -package mocks -destination mocks/task_mock.go github.com/spoke-d/thermionic/internal/cluster/heartbeat Task
//go:generate mockgen -package mocks -destination mocks/cluster_mock.go github.com/spoke-d/thermionic/internal/cluster/heartbeat Cluster
//go:generate mockgen -package mocks -destination mocks/node_mock.go github.com/spoke-d/thermionic/internal/cluster/heartbeat Node
//go:generate mockgen -package mocks -destination mocks/context_mock.go context Context
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

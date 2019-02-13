package db_test

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/golang/mock/gomock"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/query_mock.go github.com/spoke-d/thermionic/internal/db Query,QueryCluster,QueryNode,Transaction
//go:generate mockgen -package mocks -destination mocks/cluster_mock.go github.com/spoke-d/thermionic/internal/db ClusterTxProvider
//go:generate mockgen -package mocks -destination mocks/clock_mock.go github.com/spoke-d/thermionic/internal/clock Clock
//go:generate mockgen -package mocks -destination mocks/sleeper_mock.go github.com/spoke-d/thermionic/internal/clock Sleeper
//go:generate mockgen -package mocks -destination mocks/store_mock.go github.com/spoke-d/thermionic/internal/db/cluster ServerStore
//go:generate mockgen -package mocks -destination mocks/filesystem_mock.go github.com/spoke-d/thermionic/internal/fsys FileSystem
//go:generate mockgen -package mocks -destination mocks/result_mock.go database/sql Result

type nodeDestSelectObjectsMatcher struct {
	x map[int64]string
}

func NodeDestSelectObjectsMatcher(v map[int64]string) gomock.Matcher {
	return nodeDestSelectObjectsMatcher{
		x: v,
	}
}

func (m nodeDestSelectObjectsMatcher) Matches(x interface{}) bool {
	var values []db.RaftNode
	for k, v := range m.x {
		values = append(values, db.RaftNode{
			ID:      k,
			Address: v,
		})
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].ID < values[j].ID
	})

	ref := reflect.ValueOf(x)
	i := 0
	for _, v := range values {
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

type transactionMatcher struct {
	tx  database.Tx
	err error
}

func TransactionMatcher(tx database.Tx) *transactionMatcher {
	return &transactionMatcher{
		tx: tx,
	}
}

func (m *transactionMatcher) Matches(x interface{}) bool {
	fn, ok := x.(func(database.Tx) error)
	if ok {
		m.err = fn(m.tx)
		return true
	}
	return false
}

func (m *transactionMatcher) Err() error {
	return m.err
}

func (*transactionMatcher) String() string {
	return "transaction"
}

func InOrder(calls ...*gomock.Call) (last *gomock.Call) {
	for i := 1; i < len(calls); i++ {
		calls[i].After(calls[i-1])
		last = calls[i]
	}
	return
}

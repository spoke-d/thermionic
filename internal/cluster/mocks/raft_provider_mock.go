// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/spoke-d/thermionic/internal/cluster (interfaces: RaftProvider)

// Package mocks is a generated GoMock package.
package mocks

import (
	log "github.com/go-kit/kit/log"
	gomock "github.com/golang/mock/gomock"
	cert "github.com/spoke-d/thermionic/internal/cert"
	cluster "github.com/spoke-d/thermionic/internal/cluster"
	config "github.com/spoke-d/thermionic/internal/config"
	fsys "github.com/spoke-d/thermionic/internal/fsys"
	reflect "reflect"
)

// MockRaftProvider is a mock of RaftProvider interface
type MockRaftProvider struct {
	ctrl     *gomock.Controller
	recorder *MockRaftProviderMockRecorder
}

// MockRaftProviderMockRecorder is the mock recorder for MockRaftProvider
type MockRaftProviderMockRecorder struct {
	mock *MockRaftProvider
}

// NewMockRaftProvider creates a new mock instance
func NewMockRaftProvider(ctrl *gomock.Controller) *MockRaftProvider {
	mock := &MockRaftProvider{ctrl: ctrl}
	mock.recorder = &MockRaftProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockRaftProvider) EXPECT() *MockRaftProviderMockRecorder {
	return m.recorder
}

// New mocks base method
func (m *MockRaftProvider) New(arg0 cluster.Node, arg1 *cert.Info, arg2 config.Schema, arg3 fsys.FileSystem, arg4 log.Logger, arg5 float64) cluster.RaftInstance {
	ret := m.ctrl.Call(m, "New", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(cluster.RaftInstance)
	return ret0
}

// New indicates an expected call of New
func (mr *MockRaftProviderMockRecorder) New(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "New", reflect.TypeOf((*MockRaftProvider)(nil).New), arg0, arg1, arg2, arg3, arg4, arg5)
}

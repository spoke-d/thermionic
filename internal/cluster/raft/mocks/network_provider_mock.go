// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/spoke-d/thermionic/internal/cluster/raft (interfaces: NetworkProvider)

// Package mocks is a generated GoMock package.
package mocks

import (
	gomock "github.com/golang/mock/gomock"
	raft "github.com/hashicorp/raft"
	reflect "reflect"
)

// MockNetworkProvider is a mock of NetworkProvider interface
type MockNetworkProvider struct {
	ctrl     *gomock.Controller
	recorder *MockNetworkProviderMockRecorder
}

// MockNetworkProviderMockRecorder is the mock recorder for MockNetworkProvider
type MockNetworkProviderMockRecorder struct {
	mock *MockNetworkProvider
}

// NewMockNetworkProvider creates a new mock instance
func NewMockNetworkProvider(ctrl *gomock.Controller) *MockNetworkProvider {
	mock := &MockNetworkProvider{ctrl: ctrl}
	mock.recorder = &MockNetworkProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockNetworkProvider) EXPECT() *MockNetworkProviderMockRecorder {
	return m.recorder
}

// Network mocks base method
func (m *MockNetworkProvider) Network(arg0 *raft.NetworkTransportConfig) raft.Transport {
	ret := m.ctrl.Call(m, "Network", arg0)
	ret0, _ := ret[0].(raft.Transport)
	return ret0
}

// Network indicates an expected call of Network
func (mr *MockNetworkProviderMockRecorder) Network(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Network", reflect.TypeOf((*MockNetworkProvider)(nil).Network), arg0)
}
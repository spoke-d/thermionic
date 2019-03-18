// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/spoke-d/thermionic/internal/cluster/raft (interfaces: DialerProvider)

// Package mocks is a generated GoMock package.
package mocks

import (
	raft_http "github.com/CanonicalLtd/raft-http"
	gomock "github.com/golang/mock/gomock"
	cert "github.com/spoke-d/thermionic/internal/cert"
	reflect "reflect"
)

// MockDialerProvider is a mock of DialerProvider interface
type MockDialerProvider struct {
	ctrl     *gomock.Controller
	recorder *MockDialerProviderMockRecorder
}

// MockDialerProviderMockRecorder is the mock recorder for MockDialerProvider
type MockDialerProviderMockRecorder struct {
	mock *MockDialerProvider
}

// NewMockDialerProvider creates a new mock instance
func NewMockDialerProvider(ctrl *gomock.Controller) *MockDialerProvider {
	mock := &MockDialerProvider{ctrl: ctrl}
	mock.recorder = &MockDialerProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockDialerProvider) EXPECT() *MockDialerProviderMockRecorder {
	return m.recorder
}

// Dial mocks base method
func (m *MockDialerProvider) Dial(arg0 *cert.Info) (raft_http.Dial, error) {
	ret := m.ctrl.Call(m, "Dial", arg0)
	ret0, _ := ret[0].(raft_http.Dial)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Dial indicates an expected call of Dial
func (mr *MockDialerProviderMockRecorder) Dial(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Dial", reflect.TypeOf((*MockDialerProvider)(nil).Dial), arg0)
}

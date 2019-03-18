// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/spoke-d/thermionic/internal/cluster (interfaces: Net)

// Package mocks is a generated GoMock package.
package mocks

import (
	gomock "github.com/golang/mock/gomock"
	net "net"
	reflect "reflect"
)

// MockNet is a mock of Net interface
type MockNet struct {
	ctrl     *gomock.Controller
	recorder *MockNetMockRecorder
}

// MockNetMockRecorder is the mock recorder for MockNet
type MockNetMockRecorder struct {
	mock *MockNet
}

// NewMockNet creates a new mock instance
func NewMockNet(ctrl *gomock.Controller) *MockNet {
	mock := &MockNet{ctrl: ctrl}
	mock.recorder = &MockNetMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockNet) EXPECT() *MockNetMockRecorder {
	return m.recorder
}

// UnixDial mocks base method
func (m *MockNet) UnixDial(arg0 string) (net.Conn, error) {
	ret := m.ctrl.Call(m, "UnixDial", arg0)
	ret0, _ := ret[0].(net.Conn)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnixDial indicates an expected call of UnixDial
func (mr *MockNetMockRecorder) UnixDial(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnixDial", reflect.TypeOf((*MockNet)(nil).UnixDial), arg0)
}

// UnixListen mocks base method
func (m *MockNet) UnixListen(arg0 string) (net.Listener, error) {
	ret := m.ctrl.Call(m, "UnixListen", arg0)
	ret0, _ := ret[0].(net.Listener)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnixListen indicates an expected call of UnixListen
func (mr *MockNetMockRecorder) UnixListen(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnixListen", reflect.TypeOf((*MockNet)(nil).UnixListen), arg0)
}
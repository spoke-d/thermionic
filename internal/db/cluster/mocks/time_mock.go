// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/spoke-d/thermionic/internal/clock (interfaces: Sleeper)

// Package mocks is a generated GoMock package.
package mocks

import (
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
	time "time"
)

// MockSleeper is a mock of Sleeper interface
type MockSleeper struct {
	ctrl     *gomock.Controller
	recorder *MockSleeperMockRecorder
}

// MockSleeperMockRecorder is the mock recorder for MockSleeper
type MockSleeperMockRecorder struct {
	mock *MockSleeper
}

// NewMockSleeper creates a new mock instance
func NewMockSleeper(ctrl *gomock.Controller) *MockSleeper {
	mock := &MockSleeper{ctrl: ctrl}
	mock.recorder = &MockSleeperMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockSleeper) EXPECT() *MockSleeperMockRecorder {
	return m.recorder
}

// Sleep mocks base method
func (m *MockSleeper) Sleep(arg0 time.Duration) {
	m.ctrl.Call(m, "Sleep", arg0)
}

// Sleep indicates an expected call of Sleep
func (mr *MockSleeperMockRecorder) Sleep(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Sleep", reflect.TypeOf((*MockSleeper)(nil).Sleep), arg0)
}
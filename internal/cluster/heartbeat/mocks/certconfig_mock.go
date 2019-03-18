// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/spoke-d/thermionic/internal/cluster/heartbeat (interfaces: CertConfig)

// Package mocks is a generated GoMock package.
package mocks

import (
	tls "crypto/tls"
	cert "github.com/spoke-d/thermionic/internal/cert"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockCertConfig is a mock of CertConfig interface
type MockCertConfig struct {
	ctrl     *gomock.Controller
	recorder *MockCertConfigMockRecorder
}

// MockCertConfigMockRecorder is the mock recorder for MockCertConfig
type MockCertConfigMockRecorder struct {
	mock *MockCertConfig
}

// NewMockCertConfig creates a new mock instance
func NewMockCertConfig(ctrl *gomock.Controller) *MockCertConfig {
	mock := &MockCertConfig{ctrl: ctrl}
	mock.recorder = &MockCertConfigMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockCertConfig) EXPECT() *MockCertConfigMockRecorder {
	return m.recorder
}

// Read mocks base method
func (m *MockCertConfig) Read(arg0 *cert.Info) (*tls.Config, error) {
	ret := m.ctrl.Call(m, "Read", arg0)
	ret0, _ := ret[0].(*tls.Config)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read
func (mr *MockCertConfigMockRecorder) Read(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockCertConfig)(nil).Read), arg0)
}
package query_test

import (
	"fmt"
	"reflect"

	"github.com/golang/mock/gomock"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows,ColumnType
//go:generate mockgen -package mocks -destination mocks/result_mock.go database/sql Result

type intScanMatcher struct {
	x int
}

func IntScanMatcher(v int) gomock.Matcher {
	return intScanMatcher{
		x: v,
	}
}

func (m intScanMatcher) Matches(x interface{}) bool {
	ref := reflect.ValueOf(x).Elem()
	ref.Set(reflect.ValueOf(m.x))
	return true
}

func (m intScanMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

type stringScanMatcher struct {
	x string
}

func StringScanMatcher(v string) gomock.Matcher {
	return stringScanMatcher{
		x: v,
	}
}

func (m stringScanMatcher) Matches(x interface{}) bool {
	ref := reflect.ValueOf(x).Elem()
	ref.Set(reflect.ValueOf(m.x))
	return true
}

func (m stringScanMatcher) String() string {
	return m.x
}

package cluster_test

import (
	"fmt"
	"reflect"

	"github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/db/cluster/mocks"
	"github.com/golang/mock/gomock"
)

//go:generate mockgen -package mocks -destination mocks/database_mock.go github.com/spoke-d/thermionic/internal/db/cluster DatabaseRegistrar,DatabaseOpener,DatabaseDriver,DatabaseIO
//go:generate mockgen -package mocks -destination mocks/name_mock.go github.com/spoke-d/thermionic/internal/db/cluster NameProvider
//go:generate mockgen -package mocks -destination mocks/store_mock.go github.com/spoke-d/thermionic/internal/db/cluster ServerStore
//go:generate mockgen -package mocks -destination mocks/filesystem_mock.go github.com/spoke-d/thermionic/internal/fsys FileSystem
//go:generate mockgen -package mocks -destination mocks/api_extensions_mock.go github.com/spoke-d/thermionic/internal/db/cluster APIExtensions
//go:generate mockgen -package mocks -destination mocks/schema_mock.go github.com/spoke-d/thermionic/internal/db/cluster Schema,SchemaProvider
//go:generate mockgen -package mocks -destination mocks/time_mock.go github.com/spoke-d/thermionic/internal/clock Sleeper
//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/driver_mock.go database/sql/driver Driver
//go:generate mockgen -package mocks -destination mocks/result_mock.go database/sql Result

type scanMatcher struct {
	x int
}

func ScanMatcher(v int) gomock.Matcher {
	return scanMatcher{
		x: v,
	}
}

func (m scanMatcher) Matches(x interface{}) bool {
	ref := reflect.ValueOf(x).Elem()
	ref.Set(reflect.ValueOf(m.x))
	return true
}

func (m scanMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

type typeMatcher struct {
	x reflect.Kind
}

func TypeMatcher(v reflect.Kind) gomock.Matcher {
	return typeMatcher{
		x: v,
	}
}

func (m typeMatcher) Matches(x interface{}) bool {
	v := reflect.ValueOf(x)
	switch v.Kind() {
	case m.x:
		return true
	}
	return false
}

func (m typeMatcher) String() string {
	return fmt.Sprintf("%v", m.x)
}

func InOrder(calls ...*gomock.Call) (last *gomock.Call) {
	for i := 1; i < len(calls); i++ {
		calls[i].After(calls[i-1])
		last = calls[i]
	}
	return
}

func exportClusterOpen(
	ctrl *gomock.Controller,
	store cluster.ServerStore,
	mockDB *mocks.MockDB,
	databaseIO *mocks.MockDatabaseIO,
	nameProvider *mocks.MockNameProvider,
) *gomock.Call {
	mockDriver := mocks.NewMockDriver(ctrl)

	return InOrder(
		databaseIO.EXPECT().Create(store).Return(mockDriver, nil),
		nameProvider.EXPECT().DriverName().Return("driver"),
		databaseIO.EXPECT().Drivers().Return([]string{}),
		databaseIO.EXPECT().Register("driver", mockDriver),
		databaseIO.EXPECT().Open("driver", "db.bin?_foreign_keys=1").Return(mockDB, nil),
	)
}

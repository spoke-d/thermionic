package schema_test

import (
	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/schema"
	"github.com/spoke-d/thermionic/internal/db/schema/mocks"
)

//go:generate mockgen -package mocks -destination mocks/filesystem_mock.go github.com/spoke-d/thermionic/internal/fsys FileSystem
//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows,ColumnType
//go:generate mockgen -package mocks -destination mocks/result_mock.go database/sql Result

func InOrder(calls ...*gomock.Call) (last *gomock.Call) {
	for i := 1; i < len(calls); i++ {
		calls[i].After(calls[i-1])
		last = calls[i]
	}
	return
}

func expectSchemaTableExists(mockTx *mocks.MockTx, mockRows *mocks.MockRows, value int) *gomock.Call {
	return InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, value).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)
}

func expectCurrentVersion(ctrl *gomock.Controller, mockTx *mocks.MockTx, mockRows *mocks.MockRows, value int) *gomock.Call {
	mockColumnType := mocks.NewMockColumnType(ctrl)

	return InOrder(
		mockTx.EXPECT().Query(schema.StmtSelectSchemaVersions).Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
		}, nil),
		mockColumnType.EXPECT().DatabaseTypeName().Return("INTEGER"),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, value).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)
}

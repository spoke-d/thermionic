package node_test

import (
	"reflect"
	"testing"

	"github.com/spoke-d/thermionic/internal/db/node"
	"github.com/spoke-d/thermionic/internal/db/node/mocks"
	"github.com/golang/mock/gomock"
)

func TestSchemaProviderSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	schema := node.NewSchemaProviderWithMocks(mockFileSystem).Schema()
	if schema == nil {
		t.Errorf("schema should not be nil: %v", schema)
	}
}

func TestSchemaProviderUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	updates := node.NewSchemaProviderWithMocks(mockFileSystem).Updates()
	if expected, actual := 1, len(updates); expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestSchemaProviderUpdateAndExec(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	mockTx.EXPECT().Exec(TypeMatcher(reflect.String)).Return(nil, nil)

	updates := node.NewSchemaProviderWithMocks(mockFileSystem).Updates()
	err := updates[0](mockTx)
	if err != nil {
		t.Errorf("err should be nil")
	}
}

package db_test

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/mocks"
)

func TestClusterTxWithConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := map[string]string{
		"foo": "bar",
		"baz": "do",
	}
	gomock.InOrder(
		mockQuery.EXPECT().SelectConfig(mockTx, "config", "").Return(want, nil),
	)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	values, err := cluster.Config()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := want, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxWithUpdateConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := map[string]string{
		"foo": "bar",
		"baz": "do",
	}
	gomock.InOrder(
		mockQuery.EXPECT().UpdateConfig(mockTx, "config", want).Return(nil),
	)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.UpdateConfig(want)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

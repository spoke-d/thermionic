package membership_test

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/db"
)

func TestEnabled(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockNode := mocks.NewMockNode(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		matcher := NodeTransactionMatcher(nodeTx)

		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(matcher).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, "SELECT address FROM raft_nodes ORDER BY id").Return([]string{"0.0.0.0"}, nil)

		procedure := membership.NewEnabled(mockState)
		enabled, err := procedure.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if matcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if expected, actual := true, enabled; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestEnabledWithNodesEnabledFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockNode := mocks.NewMockNode(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		matcher := NodeTransactionMatcher(nodeTx)

		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(matcher).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, "SELECT address FROM raft_nodes ORDER BY id").Return([]string{}, errors.New("bad"))

		procedure := membership.NewEnabled(mockState)
		_, err := procedure.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if matcher.Err() == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

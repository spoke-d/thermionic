package node_test

import (
	"testing"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/node/mocks"
	"github.com/golang/mock/gomock"
)

func TestDetermineRaftNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	mockTx.EXPECT().Config().Return(map[string]string{
		"core.https_address": "[::]:8124",
	}, nil)
	mockTx.EXPECT().RaftNodes().Return([]db.RaftNode{
		{
			ID:      int64(2),
			Address: "[::]:8124",
		},
	}, nil)

	node, err := node.DetermineRaftNode(mockTx, node.ConfigSchema)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := int64(2), node.ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestDetermineRaftNodeWithNoMatchingAddresses(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	mockTx.EXPECT().Config().Return(map[string]string{
		"core.https_address": "[::]:8124",
	}, nil)
	mockTx.EXPECT().RaftNodes().Return([]db.RaftNode{
		{
			ID:      int64(1),
			Address: "[::]:5555",
		},
	}, nil)

	node, err := node.DetermineRaftNode(mockTx, node.ConfigSchema)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if node != nil {
		t.Errorf("expected node to be nil")
	}
}

func TestDetermineRaftNodeWithNoNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	mockTx.EXPECT().Config().Return(map[string]string{
		"core.https_address": "[::]:8124",
	}, nil)
	mockTx.EXPECT().RaftNodes().Return([]db.RaftNode{}, nil)

	node, err := node.DetermineRaftNode(mockTx, node.ConfigSchema)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := int64(1), node.ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestDetermineRaftNodeWithNoConfigHTTPSAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	mockTx.EXPECT().Config().Return(map[string]string{
		"core.https_address": "",
	}, nil)

	node, err := node.DetermineRaftNode(mockTx, node.ConfigSchema)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := int64(1), node.ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

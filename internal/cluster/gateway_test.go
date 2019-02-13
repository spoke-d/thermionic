package cluster_test

import (
	"net/http"
	"testing"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/cluster/mocks"
	"github.com/spoke-d/thermionic/internal/cluster/raft"
	dbcluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/golang/mock/gomock"
	hashiraft "github.com/hashicorp/raft"
)

func TestGatewayInitializationWithMemoryDial(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockNode := mocks.NewMockNode(ctrl)
	mockDatabase := mocks.NewMockDB(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)
	mockRaftProvider := mocks.NewMockRaftProvider(ctrl)
	mockRaft := mocks.NewMockRaftInstance(ctrl)
	mockNet := mocks.NewMockNet(ctrl)
	mockServerProvider := mocks.NewMockServerProvider(ctrl)
	mockStoreProvider := mocks.NewMockStoreProvider(ctrl)
	mockSleeper := mocks.NewMockSleeper(ctrl)
	mockListener := mocks.NewMockListener(ctrl)

	certInfo := &cert.Info{}
	mockDiskStore := mocks.NewMockServerStore(ctrl)
	mockMemoryStore := mocks.NewMockServerStore(ctrl)
	provider := raft.NewAddressProvider(mockNode)

	mockRaftProvider.EXPECT().New(mockNode, certInfo, node.ConfigSchema, mockFileSystem, gomock.Any(), 1.0).Return(mockRaft)
	mockRaft.EXPECT().Init().Return(nil)
	mockRaft.EXPECT().Raft().Return(&hashiraft.Raft{})
	mockNet.EXPECT().UnixListen("").Return(mockListener, nil)
	mockRaft.EXPECT().HandlerFunc().Return(nil)
	mockStoreProvider.EXPECT().Memory().Return(mockMemoryStore)
	mockMemoryStore.EXPECT().Set(gomock.Any(), []dbcluster.ServerInfo{
		{Address: "0"},
	}).Return(nil)
	mockServerProvider.EXPECT().New(mockRaft, mockListener, provider, gomock.Any())
	mockNode.EXPECT().DB().Return(mockDatabase)
	mockStoreProvider.EXPECT().Disk(mockDatabase).Return(mockDiskStore, nil)

	instance := cluster.NewGateway(
		mockNode,
		node.ConfigSchema,
		mockFileSystem,
		cluster.WithRaftProvider(mockRaftProvider),
		cluster.WithNet(mockNet),
		cluster.WithServerProvider(mockServerProvider),
		cluster.WithStoreProvider(mockStoreProvider),
		cluster.WithSleeper(mockSleeper),
	)
	err := instance.Init(certInfo)
	if err != nil {
		t.Errorf("expected err to be nil %v", err)
	}
}

func TestGatewayInitializationWithProxy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockNode := mocks.NewMockNode(ctrl)
	mockDatabase := mocks.NewMockDB(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)
	mockRaftProvider := mocks.NewMockRaftProvider(ctrl)
	mockRaft := mocks.NewMockRaftInstance(ctrl)
	mockNet := mocks.NewMockNet(ctrl)
	mockServerProvider := mocks.NewMockServerProvider(ctrl)
	mockStoreProvider := mocks.NewMockStoreProvider(ctrl)
	mockSleeper := mocks.NewMockSleeper(ctrl)
	mockListener := mocks.NewMockListener(ctrl)

	certInfo := &cert.Info{}
	mockDiskStore := mocks.NewMockServerStore(ctrl)
	provider := raft.NewAddressProvider(mockNode)
	mux := http.NewServeMux()

	mockRaftProvider.EXPECT().New(mockNode, certInfo, node.ConfigSchema, mockFileSystem, gomock.Any(), 1.0).Return(mockRaft)
	mockRaft.EXPECT().Init().Return(nil)
	mockRaft.EXPECT().Raft().Return(&hashiraft.Raft{})
	mockNet.EXPECT().UnixListen("").Return(mockListener, nil)
	mockRaft.EXPECT().HandlerFunc().Return(mux.ServeHTTP)
	mockServerProvider.EXPECT().New(mockRaft, mockListener, provider, gomock.Any())
	mockNode.EXPECT().DB().Return(mockDatabase)
	mockStoreProvider.EXPECT().Disk(mockDatabase).Return(mockDiskStore, nil)

	instance := cluster.NewGateway(
		mockNode,
		node.ConfigSchema,
		mockFileSystem,
		cluster.WithRaftProvider(mockRaftProvider),
		cluster.WithNet(mockNet),
		cluster.WithServerProvider(mockServerProvider),
		cluster.WithStoreProvider(mockStoreProvider),
		cluster.WithSleeper(mockSleeper),
	)
	err := instance.Init(certInfo)
	if err != nil {
		t.Errorf("expected err to be nil %v", err)
	}
}

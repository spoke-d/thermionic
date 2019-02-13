package raft_test

import (
	"os"
	"testing"
	"time"

	"github.com/CanonicalLtd/go-dqlite"

	rafthttp "github.com/CanonicalLtd/raft-http"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/raft"
	"github.com/spoke-d/thermionic/internal/cluster/raft/mocks"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/golang/mock/gomock"
	hashiraft "github.com/hashicorp/raft"
)

func TestRaftInitialization(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockNode := mocks.NewMockNode(ctrl)
	mockDialer := mocks.NewMockDialerProvider(ctrl)
	mockAddress := mocks.NewMockAddressResolver(ctrl)
	mockAddr := mocks.NewMockAddr(ctrl)
	mockHTTPProvider := mocks.NewMockHTTPProvider(ctrl2)
	mockNetworkProvider := mocks.NewMockNetworkProvider(ctrl)
	mockTransport := mocks.NewMockTransport(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)
	mockLogsProvider := mocks.NewMockLogsProvider(ctrl)
	mockLogStore := mocks.NewMockLogStore(ctrl)
	mockSnapshotStoreProvider := mocks.NewMockSnapshotStoreProvider(ctrl)
	mockSnapshotStore := mocks.NewMockSnapshotStore(ctrl)
	mockRegistryProvider := mocks.NewMockRegistryProvider(ctrl)
	mockFSM := mocks.NewMockFSM(ctrl)
	mockRaftProvider := mocks.NewMockRaftProvider(ctrl)

	mockTx := mocks.NewMockTx(ctrl2)
	mockQuery := mocks.NewMockQuery(ctrl2)
	mockMediator := mocks.NewMockRaftNodeMediator(ctrl2)

	nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
	nodeMatcher := NodeTransactionMatcher(nodeTx)

	certInfo := &cert.Info{}
	dialer := rafthttp.NewDialTCP()
	raftNode := &db.RaftNode{
		ID:      int64(1),
		Address: "10.0.0.1",
	}
	raftHandler := &rafthttp.Handler{}
	raftLayer := &rafthttp.Layer{}
	registry := &dqlite.Registry{}
	raftInst := &hashiraft.Raft{}

	mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
	mockMediator.EXPECT().DetermineRaftNode(nodeTx, node.ConfigSchema).Return(raftNode, nil)
	mockDialer.EXPECT().Dial(certInfo).Return(dialer, nil)
	mockAddress.EXPECT().Resolve("10.0.0.1").Return(mockAddr, nil)
	mockHTTPProvider.EXPECT().Handler(gomock.Not(gomock.Nil())).Return(raftHandler)
	mockHTTPProvider.EXPECT().Layer(raft.Endpoint, mockAddr, raftHandler, DialerMatcher(dialer), gomock.Any()).Return(raftLayer)
	mockNetworkProvider.EXPECT().Network(gomock.AssignableToTypeOf(&hashiraft.NetworkTransportConfig{})).Return(mockTransport)
	mockNode.EXPECT().Dir().Return("/usr/var/")
	mockFileSystem.EXPECT().Exists("/usr/var/global").Return(false)
	mockFileSystem.EXPECT().Mkdir("/usr/var/global", os.FileMode(0750)).Return(nil)
	mockLogsProvider.EXPECT().Logs("/usr/var/global/logs.db", 5*time.Second).Return(mockLogStore, nil)
	mockSnapshotStoreProvider.EXPECT().Store("/usr/var/global").Return(mockSnapshotStore, nil)
	mockLogStore.EXPECT().GetUint64([]byte("CurrentTerm")).Return(uint64(1), nil)
	mockRegistryProvider.EXPECT().Registry("/usr/var/global").Return(registry)
	mockRegistryProvider.EXPECT().FSM(registry).Return(mockFSM)
	mockRaftProvider.EXPECT().Raft(gomock.AssignableToTypeOf(&hashiraft.Config{}), mockFSM, mockLogStore, mockSnapshotStore, mockTransport).Return(raftInst, nil)

	instance := raft.New(
		mockNode,
		certInfo,
		node.ConfigSchema,
		mockFileSystem,
		raft.WithMediator(mockMediator),
		raft.WithDialerProvider(mockDialer),
		raft.WithAddressResolver(mockAddress),
		raft.WithHTTPProvider(mockHTTPProvider),
		raft.WithNetworkProvider(mockNetworkProvider),
		raft.WithLogsProvider(mockLogsProvider),
		raft.WithSnapshotStoreProvider(mockSnapshotStoreProvider),
		raft.WithRegistryProvider(mockRegistryProvider),
		raft.WithRaftProvider(mockRaftProvider),
	)
	err := instance.Init()
	if err != nil {
		t.Errorf("expected err to be nil %v", err)
	}
}

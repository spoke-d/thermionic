package heartbeat_test

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hashicorp/raft"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/heartbeat"
	"github.com/spoke-d/thermionic/internal/cluster/heartbeat/mocks"
	"github.com/spoke-d/thermionic/internal/db"
)

func TestHeartbeatRun(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		// Mocks
		mockGateway := mocks.NewMockGateway(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockTask := mocks.NewMockTask(ctrl)
		mockCertConfig := mocks.NewMockCertConfig(ctrl)
		mockContext := mocks.NewMockContext(ctrl)

		// Closure mocks
		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockResult := mocks.NewMockResult(ctrl2)

		server := httptest.NewTLSServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
			res.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ch := make(chan struct{})
		raftNodes := []db.RaftNode{
			{
				ID:      int64(0),
				Address: "0.0.0.0",
			},
		}
		certPool := x509.NewCertPool()
		certPool.AddCert(server.Certificate())

		certInfo := cert.NewInfo(tls.Certificate{}, &x509.Certificate{})
		certConfig := &tls.Config{
			RootCAs: certPool,
		}

		nodeID := int64(0)
		clusterTx := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)

		input := []db.NodeInfo{
			{
				ID:            0,
				Name:          "node1",
				Address:       server.URL,
				Description:   "node 1",
				Schema:        1,
				APIExtensions: 1,
				Heartbeat:     time.Now(),
			},
		}

		mockContext.EXPECT().Done().Return(ch)
		mockGateway.EXPECT().Clustered().Return(true)
		mockGateway.EXPECT().RaftNodes().Return(raftNodes, nil)
		mockGateway.EXPECT().DB().Return(mockNode)
		mockNode.EXPECT().Transaction(gomock.Any()).Return(nil)
		mockCluster.EXPECT().Transaction(ClusterTransactionMatcher(clusterTx)).Return(nil)
		mockQuery.EXPECT().SelectObjects(
			mockTx,
			NodeInfoDestSelectObjectsMatcher(input),
			gomock.Any(),
			0,
		).Return(nil)
		mockQuery.EXPECT().SelectStrings(
			mockTx,
			gomock.Any(),
			nodeID,
		).Return([]string{"0.0.0.0"}, nil)
		mockGateway.EXPECT().Cert().Return(certInfo)
		mockCertConfig.EXPECT().Read(certInfo).Return(certConfig, nil)
		mockContext.EXPECT().Done().Return(nil)
		mockContext.EXPECT().Err().Return(nil)
		mockCluster.EXPECT().Transaction(ClusterTransactionMatcher(clusterTx)).Return(nil)
		mockTx.EXPECT().Exec("UPDATE nodes SET heartbeat=? WHERE address=?", gomock.AssignableToTypeOf(time.Time{}), server.URL).Return(mockResult, nil)
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

		hb := heartbeat.NewHeartbeatWithMocks(mockGateway, mockCluster, mockTask, mockCertConfig)
		fn, _ := hb.Run()
		fn(mockContext)
	})
}

func TestHeartbeatRunWithNoLeader(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, _ *gomock.Controller) {
		// Mocks
		mockGateway := mocks.NewMockGateway(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockTask := mocks.NewMockTask(ctrl)
		mockCertConfig := mocks.NewMockCertConfig(ctrl)
		mockContext := mocks.NewMockContext(ctrl)

		ch := make(chan struct{})
		raftNodes := []db.RaftNode{
			{
				ID:      int64(0),
				Address: "0.0.0.0",
			},
		}

		mockContext.EXPECT().Done().Return(ch)
		mockGateway.EXPECT().Clustered().Return(true)
		mockGateway.EXPECT().RaftNodes().Return(raftNodes, raft.ErrNotLeader)

		hb := heartbeat.NewHeartbeatWithMocks(mockGateway, mockCluster, mockTask, mockCertConfig)
		fn, _ := hb.Run()
		fn(mockContext)
	})
}

func TestHeartbeatRunWithInvalidStatusCode(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		// Mocks
		mockGateway := mocks.NewMockGateway(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockTask := mocks.NewMockTask(ctrl)
		mockCertConfig := mocks.NewMockCertConfig(ctrl)
		mockContext := mocks.NewMockContext(ctrl)

		// Closure mocks
		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		server := httptest.NewTLSServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
			res.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		ch := make(chan struct{})
		raftNodes := []db.RaftNode{
			{
				ID:      int64(0),
				Address: "0.0.0.0",
			},
		}
		certPool := x509.NewCertPool()
		certPool.AddCert(server.Certificate())

		certInfo := cert.NewInfo(tls.Certificate{}, &x509.Certificate{})
		certConfig := &tls.Config{
			RootCAs: certPool,
		}

		nodeID := int64(0)
		clusterTx := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)

		input := []db.NodeInfo{
			{
				ID:            0,
				Name:          "node1",
				Address:       server.URL,
				Description:   "node 1",
				Schema:        1,
				APIExtensions: 1,
				Heartbeat:     time.Now(),
			},
		}

		mockContext.EXPECT().Done().Return(ch)
		mockGateway.EXPECT().Clustered().Return(true)
		mockGateway.EXPECT().RaftNodes().Return(raftNodes, nil)
		mockGateway.EXPECT().DB().Return(mockNode)
		mockNode.EXPECT().Transaction(gomock.Any()).Return(nil)
		mockCluster.EXPECT().Transaction(ClusterTransactionMatcher(clusterTx)).Return(nil)
		mockQuery.EXPECT().SelectObjects(
			mockTx,
			NodeInfoDestSelectObjectsMatcher(input),
			gomock.Any(),
			0,
		).Return(nil)
		mockQuery.EXPECT().SelectStrings(
			mockTx,
			gomock.Any(),
			nodeID,
		).Return([]string{"0.0.0.0"}, nil)
		mockGateway.EXPECT().Cert().Return(certInfo)
		mockCertConfig.EXPECT().Read(certInfo).Return(certConfig, nil)
		mockContext.EXPECT().Done().Return(nil)
		mockContext.EXPECT().Err().Return(nil)
		mockCluster.EXPECT().Transaction(ClusterTransactionMatcher(clusterTx)).Return(nil)

		hb := heartbeat.NewHeartbeatWithMocks(mockGateway, mockCluster, mockTask, mockCertConfig)
		fn, _ := hb.Run()
		fn(mockContext)
	})
}

package events_test

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/events"
	"github.com/spoke-d/thermionic/internal/cluster/events/mocks"
	"github.com/spoke-d/thermionic/internal/db"
	pkgevents "github.com/spoke-d/thermionic/pkg/events"
)

func TestEventsRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockEndpoints := mocks.NewMockEndpoints(ctrl)
	mockCluster := mocks.NewMockCluster(ctrl)
	mockContext := mocks.NewMockContext(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockEventsSourceProvider := mocks.NewMockEventsSourceProvider(ctrl)
	mockEventsSource := mocks.NewMockEventsSource(ctrl)

	mockTx := mocks.NewMockTx(ctrl2)
	mockQuery := mocks.NewMockQuery(ctrl2)

	ch := make(chan struct{})
	hook := func(int64, interface{}) {}

	nodeID := int64(0)
	clusterTx := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)

	certInfo := &cert.Info{}
	input := []db.NodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "https://10.0.0.1",
			Description:   "node 1",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
		{
			ID:            0,
			Name:          "node2",
			Address:       "https://10.0.0.2",
			Description:   "node 2",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
	}

	eventListener := &pkgevents.EventListener{}

	mockContext.EXPECT().Done().Return(ch)
	mockCluster.EXPECT().Transaction(ClusterTransactionMatcher(clusterTx)).Return(nil)
	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		gomock.Any(),
		0,
	).Return(nil)
	mockQuery.EXPECT().SelectStrings(mockTx, gomock.Any()).Return([]string{"10"}, nil)
	mockEndpoints.EXPECT().NetworkAddress().Return("https://10.0.0.2")
	mockClock.EXPECT().Now().Return(time.Now()).Times(2)
	mockEndpoints.EXPECT().NetworkCert().Return(certInfo)
	mockEventsSourceProvider.EXPECT().Events("https://10.0.0.1", certInfo).Return(nil, mockEventsSource, nil)
	mockEventsSource.EXPECT().GetEvents().Return(eventListener, nil)

	evt := events.New(mockEndpoints, mockCluster, hook,
		events.WithClock(mockClock),
		events.WithEventsSourceProvider(mockEventsSourceProvider),
	)
	fn, _ := evt.Run()
	fn(mockContext)
}

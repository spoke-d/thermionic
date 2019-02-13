package discovery_test

import (
	"reflect"
	"testing"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/discovery"
	"github.com/pborman/uuid"
)

func TestIntersection(t *testing.T) {
	nonce := uuid.NewRandom().String()
	tc := []struct {
		name           string
		nodes          []db.NodeInfo
		services       []db.ServiceNodeInfo
		expectedAdd    map[string]discovery.NodeData
		expectedRemove map[string]discovery.NodeData
	}{
		{
			name:           "nothing",
			nodes:          make([]db.NodeInfo, 0),
			services:       make([]db.ServiceNodeInfo, 0),
			expectedAdd:    make(map[string]discovery.NodeData),
			expectedRemove: make(map[string]discovery.NodeData),
		},
		{
			name: "1 node",
			nodes: []db.NodeInfo{
				{
					ID:      int64(1),
					Name:    "node1",
					Address: "127.0.0.1:8080",
				},
			},
			services:    make([]db.ServiceNodeInfo, 0),
			expectedAdd: make(map[string]discovery.NodeData),
			expectedRemove: map[string]discovery.NodeData{
				"127.0.0.1:8080": discovery.NodeData{
					NodeInfo: db.NodeInfo{
						ID:      int64(1),
						Name:    "node1",
						Address: "127.0.0.1:8080",
					},
				},
			},
		},
		{
			name:  "1 service",
			nodes: make([]db.NodeInfo, 0),
			services: []db.ServiceNodeInfo{
				{
					ID:            int64(1),
					Name:          "node1",
					Address:       "127.0.0.1:9081",
					DaemonAddress: "127.0.0.1:9080",
					DaemonNonce:   nonce,
				},
			},
			expectedAdd: map[string]discovery.NodeData{
				"127.0.0.1:9080": discovery.NodeData{
					NodeInfo: db.NodeInfo{
						ID:      int64(0),
						Name:    "node1",
						Address: "127.0.0.1:9081",
					},
					DaemonAddress: "127.0.0.1:9080",
					DaemonNonce:   nonce,
				},
			},
			expectedRemove: make(map[string]discovery.NodeData),
		},
		{
			name: "mixture of nodes and services",
			nodes: []db.NodeInfo{
				{
					ID:      int64(1),
					Name:    "node1",
					Address: "127.0.0.1:8080",
				},
				{
					ID:      int64(2),
					Name:    "node2",
					Address: "127.0.0.2:8080",
				},
				{
					ID:      int64(3),
					Name:    "node3",
					Address: "127.0.0.3:8080",
				},
			},
			services: []db.ServiceNodeInfo{
				{
					ID:            int64(2),
					Name:          "node2",
					Address:       "127.0.0.2:8081",
					DaemonAddress: "127.0.0.2:8080",
					DaemonNonce:   nonce,
				},
				{
					ID:            int64(4),
					Name:          "node4",
					Address:       "127.0.0.4:8081",
					DaemonAddress: "127.0.0.4:8080",
					DaemonNonce:   nonce,
				},
			},
			expectedAdd: map[string]discovery.NodeData{
				"127.0.0.4:8080": discovery.NodeData{
					NodeInfo: db.NodeInfo{
						ID:      int64(0),
						Name:    "node4",
						Address: "127.0.0.4:8081",
					},
					DaemonAddress: "127.0.0.4:8080",
					DaemonNonce:   nonce,
				},
			},
			expectedRemove: map[string]discovery.NodeData{
				"127.0.0.1:8080": discovery.NodeData{
					NodeInfo: db.NodeInfo{
						ID:      int64(1),
						Name:    "node1",
						Address: "127.0.0.1:8080",
					},
				},
				"127.0.0.3:8080": discovery.NodeData{
					NodeInfo: db.NodeInfo{
						ID:      int64(3),
						Name:    "node3",
						Address: "127.0.0.3:8080",
					},
				},
			},
		},
		{
			name: "removal of services",
			nodes: []db.NodeInfo{
				{
					ID:      int64(2),
					Name:    "node2",
					Address: "127.0.0.2:8080",
				},
			},
			services:    []db.ServiceNodeInfo{},
			expectedAdd: map[string]discovery.NodeData{},
			expectedRemove: map[string]discovery.NodeData{
				"127.0.0.2:8080": discovery.NodeData{
					NodeInfo: db.NodeInfo{
						ID:      int64(2),
						Name:    "node2",
						Address: "127.0.0.2:8080",
					},
				},
			},
		},
	}
	for _, v := range tc {
		t.Run(v.name, func(t *testing.T) {
			add, remove := discovery.Intersection(v.nodes, v.services)
			if expected, actual := v.expectedAdd, add; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := v.expectedRemove, remove; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
		})
	}
}

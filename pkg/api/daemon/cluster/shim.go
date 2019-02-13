package cluster

import (
	"context"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/db"
	querycluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/pkg/api"
)

type membershipStateShim struct {
	state api.State
}

func makeMembershipStateShim(state api.State) membershipStateShim {
	return membershipStateShim{
		state: state,
	}
}

func (s membershipStateShim) Node() membership.Node {
	return s.state.Node()
}

func (s membershipStateShim) Cluster() membership.Cluster {
	return s.state.Cluster()
}

func (s membershipStateShim) OS() membership.OS {
	return s.state.OS()
}

type membershipGatewayShim struct {
	gateway api.Gateway
}

func makeMembershipGatewayShim(gateway api.Gateway) membershipGatewayShim {
	return membershipGatewayShim{
		gateway: gateway,
	}
}

func (s membershipGatewayShim) Init(certInfo *cert.Info) error {
	return s.gateway.Init(certInfo)
}

func (s membershipGatewayShim) Shutdown() error {
	return s.gateway.Shutdown()
}

func (s membershipGatewayShim) WaitLeadership() error {
	return s.gateway.WaitLeadership()
}

func (s membershipGatewayShim) RaftNodes() ([]db.RaftNode, error) {
	return s.gateway.RaftNodes()
}

func (s membershipGatewayShim) Raft() membership.RaftInstance {
	return s.gateway.Raft()
}

func (s membershipGatewayShim) DB() membership.Node {
	return s.gateway.DB()
}

func (s membershipGatewayShim) IsDatabaseNode() bool {
	return s.gateway.IsDatabaseNode()
}

func (s membershipGatewayShim) Cert() *cert.Info {
	return s.gateway.Cert()
}

func (s membershipGatewayShim) Reset(certInfo *cert.Info) error {
	return s.gateway.Reset(certInfo)
}

func (s membershipGatewayShim) DialFunc() dqlite.DialFunc {
	return s.gateway.DialFunc()
}

func (s membershipGatewayShim) ServerStore() querycluster.ServerStore {
	return s.gateway.ServerStore()
}

func (s membershipGatewayShim) Context() context.Context {
	return s.gateway.Context()
}

type membershipEndpointsShim struct {
	endpoints api.Endpoints
}

func makeMembershipEndpointsShim(endpoints api.Endpoints) membershipEndpointsShim {
	return membershipEndpointsShim{
		endpoints: endpoints,
	}
}

func (s membershipEndpointsShim) NetworkCert() *cert.Info {
	return s.endpoints.NetworkCert()
}

func (s membershipEndpointsShim) NetworkUpdateCert(certInfo *cert.Info) error {
	return s.endpoints.NetworkUpdateCert(certInfo)
}

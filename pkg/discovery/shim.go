package discovery

import (
	"crypto/x509"

	"github.com/spoke-d/thermionic/internal/discovery"
	"github.com/spoke-d/thermionic/internal/discovery/members"
	"github.com/spoke-d/thermionic/pkg/api"
)

type membersShim struct {
	members *members.Members
}

func makeMembersShim(members *members.Members) membersShim {
	return membersShim{
		members: members,
	}
}

func (s membersShim) AddHandler(handler members.Handler) {
	s.members.AddHandler(handler)
}

func (s membersShim) RemoveHandler(handler members.Handler) {
	s.members.RemoveHandler(handler)
}

func (s membersShim) DispatchEvent(evt members.Event) error {
	return s.members.DispatchEvent(evt)
}

func (s membersShim) Join() (int, error) {
	return s.members.Join()
}

func (s membersShim) Leave() error {
	return s.members.Leave()
}

func (s membersShim) MemberList() discovery.MemberList {
	return makeMemberListShim(s.members.MemberList())
}

func (s membersShim) Walk(fn func(members.PeerInfo) error) error {
	return s.members.Walk(fn)
}

func (s membersShim) Shutdown() error {
	return s.members.Shutdown()
}

type membersListShim struct {
	memberList *members.MemberList
}

func makeMemberListShim(memberList *members.MemberList) membersListShim {
	return membersListShim{
		memberList: memberList,
	}
}

func (s membersListShim) NumMembers() int {
	return s.memberList.NumMembers()
}

func (s membersListShim) LocalNode() discovery.Member {
	return s.memberList.LocalNode()
}

func (s membersListShim) Members() []discovery.Member {
	list := s.memberList.Members()
	res := make([]discovery.Member, len(list))
	for k, v := range list {
		res[k] = v
	}
	return res
}

type discoveryShim struct {
	service *Service
}

func makeDiscoveryShim(service *Service) discoveryShim {
	return discoveryShim{
		service: service,
	}
}

func (s discoveryShim) ClientCerts() []x509.Certificate {
	return s.service.ClientCerts()
}

func (s discoveryShim) ClusterCerts() []x509.Certificate {
	return s.service.ClusterCerts()
}

func (s discoveryShim) SetupChan() <-chan struct{} {
	return s.service.SetupChan()
}

func (s discoveryShim) RegisterChan() <-chan struct{} {
	return s.service.RegisterChan()
}

func (s discoveryShim) Version() string {
	return s.service.Version()
}

func (s discoveryShim) Endpoints() api.Endpoints {
	return s.service.Endpoints()
}

func (s discoveryShim) UnsafeShutdown() {
	s.service.UnsafeShutdown()
}

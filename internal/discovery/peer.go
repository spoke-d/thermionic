package discovery

import (
	"time"

	"github.com/spoke-d/thermionic/internal/discovery/members"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

const (
	defaultBroadcastTimeout         = time.Second * 10
	defaultMembersBroadcastInterval = time.Second * 5
	defaultLowMembersThreshold      = 1
)

// EventBus allows the distributing and receiving of events over the cluster
type EventBus interface {

	// AddHandler attaches a event listener to all the members events
	// and broadcasts the event to the handler.
	AddHandler(members.Handler)

	// RemoveHandler removes the event listener.
	RemoveHandler(members.Handler)

	// DispatchEvent dispatches an event to all the members in the cluster.
	DispatchEvent(members.Event) error
}

// Members represents a way of joining a members cluster
type Members interface {
	EventBus

	// Join joins an existing members cluster. Returns the number of nodes
	// successfully contacted. The returned error will be non-nil only in the
	// case that no nodes could be contacted.
	Join() (int, error)

	// Leave gracefully exits the cluster. It is safe to call this multiple
	// times.
	Leave() error

	// Memberlist is used to get access to the underlying Memberlist instance
	MemberList() MemberList

	// Walk over a set of alive members
	Walk(func(members.PeerInfo) error) error

	// Shutdown the current members cluster
	Shutdown() error
}

// MemberList represents a way to manage members with in a cluster
type MemberList interface {

	// NumMembers returns the number of alive nodes currently known. Between
	// the time of calling this and calling Members, the number of alive nodes
	// may have changed, so this shouldn't be used to determine how many
	// members will be returned by Members.
	NumMembers() int

	// LocalNode is used to return the local Member
	LocalNode() Member

	// Members returns a point-in-time snapshot of the members of this cluster.
	Members() []Member
}

// Member represents a node in the cluster.
type Member interface {

	// Name returns the name of the member
	Name() string

	// Address returns the host:port of the member
	Address() string
}

// Peer represents the node with in the cluster.
type Peer struct {
	members Members
	logger  log.Logger
}

// NewPeer creates or joins a cluster with the existing peers.
// We will listen for cluster communications on the bind addr:port.
// We advertise a PeerType HTTP API, reachable on apiPort.
func NewPeer(
	members Members,
	logger log.Logger,
) *Peer {
	return &Peer{
		members: members,
		logger:  logger,
	}
}

// Close out the API
func (p *Peer) Close() {}

// Join attempts to join the cluster
func (p *Peer) Join() (int, error) {
	numNodes, err := p.members.Join()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return numNodes, nil
}

// Leave the cluster.
func (p *Peer) Leave() error {
	// Ignore this timeout for now, serf uses a config timeout.
	return p.members.Leave()
}

// Name returns unique ID of this peer in the cluster.
func (p *Peer) Name() string {
	return p.members.MemberList().LocalNode().Name()
}

// Address returns host:port of this peer in the cluster.
func (p *Peer) Address() string {
	return p.members.MemberList().LocalNode().Address()
}

// ClusterSize returns the total size of the cluster from this node's
// perspective.
func (p *Peer) ClusterSize() int {
	return p.members.MemberList().NumMembers()
}

// State returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) State() map[string]interface{} {
	members := p.members.MemberList()
	return map[string]interface{}{
		"self":        members.LocalNode().Name(),
		"members":     memberNames(members.Members()),
		"num_members": members.NumMembers(),
	}
}

// Current API host:ports for the given type of node.
// IncludeLocal doesn't add the local cluster node to the resulting set.
func (p *Peer) Current(peerType members.PeerType, includeLocal bool) (res []string, err error) {
	localName := p.Name()
	err = p.members.Walk(func(info members.PeerInfo) error {
		if !includeLocal && info.Name == localName {
			return nil
		}

		if peerType == PeerTypeStore && info.Type == PeerTypeStore {
			res = append(res, info.DaemonAddress)
		}
		return nil
	})
	return
}

// AddHandler attaches an event listener to all the members events
// and broadcasts the event to the handler.
func (p *Peer) AddHandler(handler members.Handler) {
	p.members.AddHandler(handler)
}

// RemoveHandler removes the event listener.
func (p *Peer) RemoveHandler(handler members.Handler) {
	p.members.RemoveHandler(handler)
}

// DispatchEvent dispatches an event to all the members in the cluster.
func (p *Peer) DispatchEvent(e members.Event) error {
	return p.members.DispatchEvent(e)
}

func memberNames(m []Member) []string {
	res := make([]string, len(m))
	for k, v := range m {
		res[k] = v.Name()
	}
	return res
}

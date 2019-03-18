package members

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/cmd/serf/command/agent"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
)

const (
	defaultAgentLogLevel = "WARN"
)

// Handler handles any events coming from the underlying serf agent
type Handler interface {
	HandleEvent(Event)
}

// Members represents a way of joining a members cluster
type Members struct {
	agent    *agent.Agent
	members  *serf.Serf
	existing []string
	mutex    sync.Mutex
	handlers map[Handler]agent.EventHandler
	logger   log.Logger
}

// NewMembers creates a new members list to join.
func NewMembers(options ...Option) *Members {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Members{
		existing: opts.existing,
		handlers: make(map[Handler]agent.EventHandler),
		logger:   opts.logger,
	}
}

// Init will initialise the members with the config to start the agent.
func (m *Members) Init(config ...Config) error {
	actor, err := agent.Create(configTransform(config...))
	if err != nil {
		return errors.WithStack(err)
	}

	if err := actor.Start(); err != nil {
		return errors.WithStack(err)
	}

	m.mutex.Lock()
	m.agent = actor
	m.members = actor.Serf()
	m.mutex.Unlock()

	return nil
}

// Join joins an existing Serf cluster. Returns the number of nodes
// successfully contacted.
func (m *Members) Join() (int, error) {
	return m.members.Join(m.existing, true)
}

// Leave gracefully exits the cluster. It is safe to call this multiple
// times.
func (m *Members) Leave() error {
	return m.members.Leave()
}

// MemberList is used to get access to the underlying Memberlist instance
func (m *Members) MemberList() *MemberList {
	return &MemberList{
		list:   m.members.Memberlist(),
		logger: m.logger,
	}
}

// Walk over the list of members to gain access to each peer information
func (m *Members) Walk(fn func(PeerInfo) error) error {
	for _, v := range m.members.Members() {
		if v.Status != serf.StatusAlive {
			continue
		}

		if info, err := decodePeerInfoTag(v.Tags); err == nil {
			if e := fn(info); e != nil {
				return err
			}
		}
	}
	return nil
}

// AddHandler attaches an event listener to all the members events
// and broadcasts the event to the handler.
func (m *Members) AddHandler(handler Handler) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if h, ok := m.handlers[handler]; ok {
		delete(m.handlers, handler)
		m.agent.DeregisterEventHandler(h)
	}

	eventHandler := &EventHandler{
		handler: handler,
		logger:  log.With(m.logger, "component", "event-handler"),
	}
	m.handlers[handler] = eventHandler
	m.agent.RegisterEventHandler(eventHandler)
}

// RemoveHandler removes the event listener.
func (m *Members) RemoveHandler(handler Handler) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if h, ok := m.handlers[handler]; ok {
		delete(m.handlers, handler)
		m.agent.DeregisterEventHandler(h)
	}
}

// DispatchEvent dispatches an event to all the members in the cluster.
func (m *Members) DispatchEvent(e Event) error {
	switch e.Type() {
	case EventUser:
		evt, ok := e.(*UserEvent)
		if !ok {
			return errors.Errorf("unsupported user event")
		}
		return m.agent.UserEvent(evt.Name, evt.Payload, true)
	default:
		return errors.Errorf("unsupported event type %v", e.Type())
	}
}

// Shutdown will leave the cluster gracefully, before shutting it down.
func (m *Members) Shutdown() error {
	if err := m.members.Leave(); err != nil {
		return errors.WithStack(err)
	}
	if err := m.members.Shutdown(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// MemberList represents a way to manage members with in a cluster
type MemberList struct {
	list   *memberlist.Memberlist
	logger log.Logger
}

// NumMembers returns the number of alive nodes currently known. Between
// the time of calling this and calling Members, the number of alive nodes
// may have changed, so this shouldn't be used to determine how many
// members will be returned by Members.
func (m *MemberList) NumMembers() int {
	return m.list.NumMembers()
}

// LocalNode is used to return the local Member
func (m *MemberList) LocalNode() Member {
	return Member{
		member: m.list.LocalNode(),
	}
}

// Members returns a point-in-time snapshot of the members of this cluster.
func (m *MemberList) Members() []Member {
	members := m.list.Members()
	res := make([]Member, len(members))
	for k, v := range members {
		res[k] = Member{
			member: v,
		}
	}
	return res
}

// Member represents a node in the cluster.
type Member struct {
	member *memberlist.Node
}

// Name returns the name of the member
func (r Member) Name() string {
	return r.member.Name
}

// Address returns the host:port of the member
func (r Member) Address() string {
	return r.member.Address()
}

// PeerInfo returns the meta data associated with a node
func (r Member) PeerInfo() (PeerInfo, error) {
	var info PeerInfo
	if err := json.NewDecoder(bytes.NewReader(r.member.Meta)).Decode(&info); err != nil {
		return PeerInfo{}, errors.WithStack(err)
	}
	return info, nil
}

// EventHandler defines a handler for dealing with events coming from the
// underlying serf agent
type EventHandler struct {
	handler Handler
	logger  log.Logger
}

// HandleEvent handles any events from the underlying serf agent.
func (h *EventHandler) HandleEvent(event serf.Event) {
	switch event.EventType() {
	case serf.EventMemberJoin,
		serf.EventMemberLeave,
		serf.EventMemberFailed,
		serf.EventMemberUpdate,
		serf.EventMemberReap:
		h.handleMemberEvent(event.(serf.MemberEvent))
		return
	case serf.EventUser:
		if userEvent, ok := event.(serf.UserEvent); ok {
			h.handler.HandleEvent(NewUserEvent(userEvent.Name, userEvent.Payload))
			return
		}
	case serf.EventQuery:
		if serfQuery, ok := event.(*serf.Query); ok {
			h.handler.HandleEvent(NewQueryEvent(serfQuery.Name, serfQuery.Payload, serfQuery))
			return
		}
	}
	h.handler.HandleEvent(NewErrorEvent(errors.New("unexpected event")))
}

func (h *EventHandler) handleMemberEvent(event serf.MemberEvent) {
	var t MemberEventType
	switch event.EventType() {
	case serf.EventMemberJoin:
		t = EventMemberJoined
	case serf.EventMemberFailed:
		t = EventMemberFailed
	case serf.EventMemberLeave:
		t = EventMemberLeft
	case serf.EventMemberUpdate:
		t = EventMemberUpdated
	}

	m := make([]Member, len(event.Members))
	for k, v := range event.Members {
		var meta []byte
		if bytes, err := json.Marshal(v.Tags); err == nil {
			meta = bytes
		}

		m[k] = Member{
			member: &memberlist.Node{
				Name: v.Name,
				Addr: v.Addr,
				Port: v.Port,
				Meta: meta,
				PMin: v.ProtocolMin,
				PMax: v.ProtocolMax,
				PCur: v.ProtocolCur,
				DMin: v.DelegateMin,
				DMax: v.DelegateMax,
				DCur: v.DelegateCur,
			},
		}
	}

	h.handler.HandleEvent(NewMemberEvent(t, m))
}

func configTransform(configs ...Config) (*agent.Config, *serf.Config, io.Writer) {
	cfgs := newConfigs()
	for _, config := range configs {
		config(cfgs)
	}

	agentConfig := agent.DefaultConfig()
	agentConfig.LogLevel = defaultAgentLogLevel
	if cfgs.clientAddr != "" {
		agentConfig.RPCAddr = fmt.Sprintf("%s:%d", cfgs.clientAddr, cfgs.clientPort)
	}

	serfConfig := serf.DefaultConfig()
	serfConfig.NodeName = cfgs.nodeName
	serfConfig.MemberlistConfig.BindAddr = cfgs.bindAddr
	serfConfig.MemberlistConfig.BindPort = cfgs.bindPort
	if cfgs.advertiseAddr != "" {
		serfConfig.MemberlistConfig.AdvertiseAddr = cfgs.advertiseAddr
		serfConfig.MemberlistConfig.AdvertisePort = cfgs.advertisePort
	}
	serfConfig.MemberlistConfig.LogOutput = cfgs.logOutput
	serfConfig.LogOutput = cfgs.logOutput
	serfConfig.BroadcastTimeout = cfgs.broadcastTimeout
	serfConfig.Tags = encodePeerInfoTag(PeerInfo{
		Name:          cfgs.nodeName,
		Type:          cfgs.peerType,
		DaemonAddress: cfgs.daemonAddress,
		DaemonNonce:   cfgs.daemonNonce,
	})
	serfConfig.Init()

	return agentConfig, serfConfig, cfgs.logOutput
}

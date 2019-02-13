package discovery

import (
	"crypto/x509"
	"fmt"
	"os/user"
	"time"

	"github.com/spoke-d/thermionic/client"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/discovery"
	"github.com/spoke-d/thermionic/internal/discovery/members"
	"github.com/spoke-d/thermionic/internal/endpoints"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/retrier"
	"github.com/spoke-d/thermionic/internal/task"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
)

// Endpoints are in charge of bringing up and down the HTTP endpoints for
// serving the RESTful API.
type Endpoints interface {

	// Up brings up all configured endpoints and starts accepting HTTP requests.
	Up() error

	// Down brings down all endpoints and stops serving HTTP requests.
	Down() error

	// NetworkAddress returns the network address of the network endpoint, or an
	// empty string if there's no network endpoint
	NetworkAddress() string

	// NetworkPrivateKey returns the private key of the TLS certificate used by the
	// network endpoint.
	NetworkPrivateKey() []byte

	// NetworkCert returns the full TLS certificate information for this endpoint.
	NetworkCert() *cert.Info

	// NetworkPublicKey returns the public key of the TLS certificate used by the
	// network endpoint.
	NetworkPublicKey() []byte

	// NetworkUpdateAddress updates the address for the network endpoint,
	// shutting it down and restarting it.
	NetworkUpdateAddress(string) error

	// PprofUpdateAddress updates the address for the pprof endpoint, shutting
	// it down and restarting it.
	PprofUpdateAddress(string) error

	// NetworkUpdateCert updates the cert for the network endpoint,
	// shutting it down and restarting it.
	NetworkUpdateCert(*cert.Info) error
}

// Peer represents the node with in the cluster.
type Peer interface {
	// AddHandler attaches a event listener to all the members events
	// and broadcasts the event to the handler.
	AddHandler(members.Handler)

	// RemoveHandler removes the event listener.
	RemoveHandler(members.Handler)

	// Join the cluster
	Join() (int, error)

	// Leave the cluster.
	Leave() error

	// Name returns unique ID of this peer in the cluster.
	Name() string

	// Address returns host:port of this peer in the cluster.
	Address() string

	// ClusterSize returns the total size of the cluster from this node's
	// perspective.
	ClusterSize() int

	// State returns a JSON-serializable dump of cluster state.
	// Useful for debug.
	State() map[string]interface{}

	// Current API host:ports for the given type of node.
	// Bool defines if you want to include the current local node.
	Current(members.PeerType, bool) ([]string, error)

	// Close and shutdown the peer
	Close()
}

// OS is a high-level facade for accessing all operating-system
// level functionality that therm uses.
type OS interface {

	// Init our internal data structures.
	Init(fsys.FileSystem) error

	// LocalDatabasePath returns the path of the local database file.
	LocalDatabasePath() string

	// GlobalDatabaseDir returns the path of the global database directory.
	GlobalDatabaseDir() string

	// GlobalDatabasePath returns the path of the global database SQLite file
	// managed by dqlite.
	GlobalDatabasePath() string

	// VarDir represents the Data directory (e.g. /var/lib/therm/).
	VarDir() string

	// Hostname returns the host name reported by the kernel.
	Hostname() (string, error)

	// HostNames will generate a list of names for which the certificate will be
	// valid.
	// This will include the hostname and ip address
	HostNames() ([]string, error)

	// User returns the current user.
	User() (*user.User, error)
}

// SchedulerTask defines a task that can be run repeatedly
type SchedulerTask interface {

	// Run setups up the given schedule
	Run() (task.Func, task.Schedule)
}

// Service defines a controller for managing service discovery
type Service struct {
	certInfo                   *cert.Info
	clientCerts, clusterCerts  []x509.Certificate
	config                     *clusterconfig.ReadOnlyConfig
	version                    string
	peer                       Peer
	os                         OS
	daemonAddress, daemonNonce string
	daemonClient               *client.Services

	networkAddress string
	debugAddress   string

	eventHandler eventHandler

	// Tasks registry for long-running background tasks.
	tasks *task.Group

	setupChan    chan struct{}
	registerChan chan struct{}
	shutdownChan chan struct{}

	fileSystem fsys.FileSystem
	logger     log.Logger
	sleeper    clock.Sleeper

	endpoints Endpoints

	apiServices         []api.Service
	apiInternalServices []api.Service
}

// NewService creates a new service with sane defaults
func NewService(
	certInfo *cert.Info,
	clientCerts, clusterCerts []x509.Certificate,
	config *clusterconfig.ReadOnlyConfig,
	version string,
	networkAddress, debugAddress string,
	apiServices, apiInternalServices []api.Service,
	daemonAddress, daemonNonce string,
	options ...Option,
) *Service {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Service{
		certInfo:            certInfo,
		clientCerts:         clientCerts,
		clusterCerts:        clusterCerts,
		config:              config,
		version:             version,
		networkAddress:      networkAddress,
		debugAddress:        debugAddress,
		tasks:               task.NewGroup(),
		setupChan:           make(chan struct{}),
		registerChan:        make(chan struct{}),
		shutdownChan:        make(chan struct{}),
		apiServices:         apiServices,
		apiInternalServices: apiInternalServices,
		daemonAddress:       daemonAddress,
		daemonNonce:         daemonNonce,
		os:                  opts.os,
		fileSystem:          opts.fileSystem,
		logger:              opts.logger,
		sleeper:             opts.sleeper,
	}
}

// Init the Service, creating all the required dependencies.
func (s *Service) Init(configs ...Config) error {
	cfgs := newConfigs()
	for _, config := range configs {
		config(cfgs)
	}

	if err := s.init(cfgs); err != nil {
		level.Error(s.logger).Log("msg", "failed to start the service", "err", err)
		s.Stop()
		return errors.WithStack(err)
	}
	return nil
}

// Register tasks or system services for the service
func (s *Service) Register(tasks []SchedulerTask) error {
	for _, task := range tasks {
		s.tasks.Add(task.Run())
	}

	s.tasks.Start()

	level.Info(s.logger).Log("msg", "registered tasks")
	close(s.registerChan)

	return nil
}

// ClientCerts returns the certificates used locally
func (s *Service) ClientCerts() []x509.Certificate {
	return s.clientCerts
}

// ClusterCerts returns the network certificates used for the cluster.
func (s *Service) ClusterCerts() []x509.Certificate {
	return s.clusterCerts
}

// SetupChan returns a channel that blocks until setup has happened from
// the Service
func (s *Service) SetupChan() <-chan struct{} {
	return s.setupChan
}

// RegisterChan returns a channel that blocks until all the registered tasks
// have happened for the Service
func (s *Service) RegisterChan() <-chan struct{} {
	return s.registerChan
}

// ShutdownChan returns a channel that blocks until shutdown has happened from
// the Service.
func (s *Service) ShutdownChan() <-chan struct{} {
	return s.shutdownChan
}

// Version returns the current version of the daemon
func (s *Service) Version() string {
	return s.version
}

// Endpoints returns the underlying endpoints that the daemon controls.
func (s *Service) Endpoints() Endpoints {
	return s.endpoints
}

// Stop stops the shared service.
func (s *Service) Stop() error {
	level.Info(s.logger).Log("msg", "starting shutdown sequence")

	// Track all the errors, if there is an error continue stopping.
	var errs []error
	trackError := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if s.endpoints != nil {
		trackError(s.endpoints.Down())
	}

	// Give tasks a bit of time to cleanup.
	trackError(s.tasks.Stop(3 * time.Second))

	if s.peer != nil {
		if err := s.peer.Leave(); err != nil {
			return errors.WithStack(err)
		}
		s.peer.RemoveHandler(s.eventHandler)
		s.peer.Close()
	}

	var err error
	if n := len(errs); n > 0 {
		format := "%v"
		if n > 1 {
			format += fmt.Sprintf(" (and %d more errors)", n)
		}
		err = errors.Errorf(format, errs[0])
	}
	if err != nil {
		level.Error(s.logger).Log("msg", "failed to cleanly shutdown", "err", err)
	}
	return errors.WithStack(err)
}

// Kill signals the service that we want to shutdown
func (s *Service) Kill() {
}

func (s *Service) init(cfg *config) error {
	level.Info(s.logger).Log("msg", "starting service")

	mbrs := members.NewMembers(
		members.WithLogger(log.With(s.logger, "component", "members")),
	)
	if err := mbrs.Init(
		members.WithPeerType(discovery.PeerTypeStore),
		members.WithNodeName(uuid.NewRandom().String()),
		members.WithDaemon(cfg.daemonAddress, cfg.daemonNonce),
		members.WithBindAddrPort(cfg.bindAddr, cfg.bindPort),
		members.WithAdvertiseAddrPort(cfg.advertiseAddr, cfg.advertisePort),
		members.WithLogOutput(membersLogOutput{
			logger: log.With(s.logger, "component", "cluster"),
		}),
	); err != nil {
		return errors.WithStack(err)
	}

	peer := discovery.NewPeer(makeMembersShim(mbrs), log.With(s.logger, "component", "peer"))
	if _, err := peer.Join(); err != nil {
		return errors.WithStack(err)
	}
	s.peer = peer

	s.eventHandler = eventHandler{
		service: s,
		logger:  log.WithPrefix(s.logger, "component", "event-handler"),
	}
	s.peer.AddHandler(s.eventHandler)

	restServer, err := api.DiscoveryRestServer(
		makeDiscoveryShim(s),
		s.config,
		s.apiServices,
		s.apiInternalServices,
		api.WithLogger(log.WithPrefix(s.logger, "component", "api")),
	)
	if err != nil {
		return errors.WithStack(err)
	}

	s.endpoints = endpoints.New(
		restServer,
		s.certInfo,
		endpoints.WithNetworkAddress(s.networkAddress),
		endpoints.WithDebugAddress(s.debugAddress),
		endpoints.WithLogger(log.WithPrefix(s.logger, "component", "endpoints")),
	)
	if err := s.endpoints.Up(); err != nil {
		return errors.Wrap(err, "failed to setup API endpoints")
	}

	// go create a daemon client using the nonce.
	c, err := client.NewUntrusted(
		s.daemonAddress,
		s.daemonNonce,
		client.WithLogger(log.WithPrefix(s.logger, "component", "discovery-client", "action", "join")),
	)
	if err != nil {
		return errors.Wrap(err, "unable to construct untrusted client")
	}

	info, err := c.Info().Get()
	if err != nil {
		return errors.Wrap(err, "error requesting daemon information")
	}

	client, err := client.New(
		s.daemonAddress,
		info.Environment.Certificate,
		info.Environment.Certificate,
		info.Environment.CertificateKey,
	)
	if err != nil {
		return errors.Wrap(err, "unable to construct client")
	}
	s.daemonClient = client.Discovery().Services()

	close(s.setupChan)

	return s.Register([]SchedulerTask{})
}

func (s *Service) handleMemberJoined(members []members.Member) {
	select {
	case <-s.setupChan:
	}

	nodes := make([]client.ServiceNode, len(members))
	for k, v := range members {
		info, err := v.PeerInfo()
		if err != nil {
			continue
		}

		nodes[k] = client.ServiceNode{
			ServerName:    v.Name(),
			ServerAddress: v.Address(),
			DaemonAddress: info.DaemonAddress,
			DaemonNonce:   info.DaemonNonce,
		}
	}

	retry := retrier.New(s.sleeper, 3, time.Second)
	if err := retry.Run(func() error {
		return s.daemonClient.Add(nodes)
	}); err != nil {
		level.Error(s.logger).Log("msg", "failed to join members", "err", err)
	}
}

func (s *Service) handleMemberLeft(members []members.Member) {
	select {
	case <-s.setupChan:
	}

	nodes := make([]client.ServiceNode, len(members))
	for k, v := range members {
		info, err := v.PeerInfo()
		if err != nil {
			continue
		}

		nodes[k] = client.ServiceNode{
			ServerName:    v.Name(),
			ServerAddress: v.Address(),
			DaemonAddress: info.DaemonAddress,
			DaemonNonce:   info.DaemonNonce,
		}
	}

	retry := retrier.New(s.sleeper, 3, time.Second)
	if err := retry.Run(func() error {
		return s.daemonClient.Delete(nodes)
	}); err != nil {
		level.Error(s.logger).Log("msg", "failed to remove members", "err", err)
	}
}

// UnsafeShutdown forces an automatic shutdown of the Daemon
func (s *Service) UnsafeShutdown() {
	s.shutdownChan <- struct{}{}
}

type membersLogOutput struct {
	logger log.Logger
}

func (m membersLogOutput) Write(b []byte) (int, error) {
	level.Debug(m.logger).Log("msg", string(b))
	return len(b), nil
}

type eventHandler struct {
	service *Service
	logger  log.Logger
}

func (e eventHandler) HandleEvent(event members.Event) {
	level.Debug(e.logger).Log("msg", "handle event", "event", fmt.Sprintf("%+v", event))

	switch event.Type() {
	case members.EventMember:
		if memberEvent, ok := event.(*members.MemberEvent); ok {
			switch memberEvent.EventType {
			case members.EventMemberJoined:
				e.service.handleMemberJoined(memberEvent.Members)
			case members.EventMemberLeft:
				e.service.handleMemberLeft(memberEvent.Members)
			}
		}
	}
}

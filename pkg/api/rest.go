package api

import (
	"context"
	"crypto/x509"
	"net/http"
	"os/user"
	"strings"
	"sync"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/spoke-d/thermionic/internal/cert"
	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	querycluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/operations"
	"github.com/spoke-d/thermionic/internal/schedules"
	"github.com/spoke-d/thermionic/internal/task"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// Interval represents the number of seconds to wait between to heartbeat
// rounds.
const Interval = 60

// Gateway mediates access to the dqlite cluster using a gRPC SQL client, and
// possibly runs a dqlite replica on this node (if we're configured to do
// so).
type Gateway interface {

	// Init the gateway, creating a new raft factory and gRPC server (if this
	// node is a database node), and a gRPC dialer.
	Init(*cert.Info) error

	// Shutdown this gateway, stopping the gRPC server and possibly the raft
	// factory.
	Shutdown() error

	// WaitLeadership will wait for the raft node to become leader. Should only
	// be used by Bootstrap, since we know that we'll self elect.
	WaitLeadership() error

	// RaftNodes returns information about the nodes that a currently
	// part of the raft cluster, as configured in the raft log. It returns an
	// error if this node is not the leader.
	RaftNodes() ([]db.RaftNode, error)

	// Raft returns the raft instance
	Raft() RaftInstance

	// HandlerFuncs returns the HTTP handlers that should be added to the REST API
	// endpoint in order to handle database-related requests.
	HandlerFuncs() map[string]http.HandlerFunc

	// DB returns the the underlying db node
	DB() Node

	// IsDatabaseNode returns true if this gateway also run acts a raft database
	// node.
	IsDatabaseNode() bool

	// LeaderAddress returns the address of the current raft leader.
	LeaderAddress() (string, error)

	// Cert returns the currently available cert in the gateway
	Cert() *cert.Info

	// Reset the gateway, shutting it down and starting against from scratch
	// using the given certificate.
	//
	// This is used when disabling clustering on a node.
	Reset(cert *cert.Info) error

	// DialFunc returns a dial function that can be used to connect to one of
	// the dqlite nodes.
	DialFunc() dqlite.DialFunc

	// ServerStore returns a dqlite server store that can be used to lookup the
	// addresses of known database nodes.
	ServerStore() querycluster.ServerStore

	// Context returns a cancellation context to pass to dqlite.NewDriver as
	// option.
	//
	// This context gets cancelled by Gateway.Kill() and at that point any
	// connection failure won't be retried.
	Context() context.Context
}

// RaftInstance is a specific wrapper around raft.Raft, which also holds a
// reference to its network transport and dqlite FSM.
type RaftInstance interface {

	// MembershipChanger returns the underlying rafthttp.Layer, which can be
	// used to change the membership of this node in the cluster.
	MembershipChanger() raftmembership.Changer
}

// Cluster mediates access to data stored in the cluster dqlite database.
type Cluster interface {
	database.DBAccessor
	db.ClusterOpener
	db.ClusterTransactioner
	db.ClusterExclusiveLocker

	// NodeID sets the the node NodeID associated with this cluster instance. It's used for
	// backward-compatibility of all db-related APIs that were written before
	// clustering and don't accept a node NodeID, so in those cases we automatically
	// use this value as implicit node NodeID.
	NodeID(int64)

	// SchemaVersion returns the underlying schema version for the cluster
	SchemaVersion() int

	// Close the database facade.
	Close() error
}

// Node mediates access to the data stored locally
type Node interface {
	database.DBAccessor
	db.NodeOpener
	db.NodeTransactioner

	// Dir returns the directory of the underlying database file.
	Dir() string

	// Close the database facade.
	Close() error
}

// Endpoints are in charge of bringing up and down the HTTP endpoints for
// serving the RESTful API.
type Endpoints interface {

	// NetworkPublicKey returns the public key of the TLS certificate used by the
	// network endpoint.
	NetworkPublicKey() []byte

	// NetworkPrivateKey returns the private key of the TLS certificate used by the
	// network endpoint.
	NetworkPrivateKey() []byte

	// NetworkCert returns the full TLS certificate information for this endpoint.
	NetworkCert() *cert.Info

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

// OS is a high-level facade for accessing all operating-system
// level functionality that therm uses.
type OS interface {

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

// State is a gateway to the two main stateful components, the database
// and the operating system. It's typically used by model entities in order to
// perform changes.
type State interface {
	// Node returns the underlying Node
	Node() Node

	// Cluster returns the underlying Cluster
	Cluster() Cluster

	// OS returns the underlying OS values
	OS() OS
}

// Operations defines an interface for interacting with a series of operations
type Operations interface {

	// Add an operation to the collection
	Add(*operations.Operation) error

	// GetOpByPartialID retrieves an op of the operation from the collection by a
	// partial id. As long as the prefix matches an id then it will return that
	// operation. If the id matches multiple ids then it will return an ambiguous
	// error.
	GetOpByPartialID(string) (operations.Op, error)

	// DeleteOp attempts to kill an operation by the id
	DeleteOp(string) error

	// WaitOp for an operation to be completed
	WaitOp(string, time.Duration) (bool, error)

	// Walk over a collection of operations
	Walk(func(operations.Op) error) error
}

// Schedules defines an interface for interacting with a set of scheduled tasks
type Schedules interface {

	// Add an task to the schedule
	Add(*schedules.Task) error

	// Remove a scheduled task
	Remove(string) error

	// GetTsk returns a scheduled task
	GetTsk(string) (schedules.Tsk, error)

	// Walk over a set of scheduled tasks
	Walk(func(schedules.Tsk) error) error
}

// Facade defines a facade to interact with the underlying controller.
type Facade interface {
	// SetupChan returns a channel that blocks until setup has happened from
	// the Daemon
	SetupChan() <-chan struct{}

	// RegisterChan returns a channel that blocks until all the registered tasks
	// have happened for the Daemon
	RegisterChan() <-chan struct{}

	// ClientCerts returns the associated client certificates
	ClientCerts() []x509.Certificate

	// ClusterCerts returns the associated client certificates
	ClusterCerts() []x509.Certificate
}

// Daemon can respond to requests from a shared client.
type Daemon interface {
	Facade

	// Gateway returns the underlying Daemon Gateway
	Gateway() Gateway

	// Cluster returns the underlying Cluster
	Cluster() Cluster

	// Node returns the underlying Node associated with the daemon
	Node() Node

	// State creates a new State instance liked to our internal db and os.
	State() State

	// Operations return the underlying operational tasks associated with the
	// current daemon
	Operations() Operations

	// Schedules return the underlying schedule tasks associated with the
	// cluster
	Schedules() Schedules

	// ClusterConfigSchema returns the daemon schema for the Cluster
	ClusterConfigSchema() config.Schema

	// NodeConfigSchema returns the daemon schema for the local Node
	NodeConfigSchema() config.Schema

	// ActorGroup returns the actor group for event broadcast
	ActorGroup() ActorGroup

	// EventBroadcaster returns a event broadcaster for event processing
	EventBroadcaster() EventBroadcaster

	// Version returns the current version of the daemon
	Version() string

	// Nonce returns the current nonce of the daemon
	Nonce() string

	// Endpoints returns the underlying endpoints that the daemon controls.
	Endpoints() Endpoints

	// APIExtensions returns the extensions available to the current daemon
	APIExtensions() []string

	// UnsafeShutdown forces an automatic shutdown of the Daemon
	UnsafeShutdown()

	// UnsafeSetCluster forces a cluster onto the daemon.
	UnsafeSetCluster(Cluster)
}

// Discovery can respond to requests from a shared client.
type Discovery interface {
	Facade

	// Version returns the current version of the discovery
	Version() string

	// Endpoints returns the underlying endpoints that the discovery controls.
	Endpoints() Endpoints

	// UnsafeShutdown forces an automatic shutdown of the Discovery
	UnsafeShutdown()
}

// Actor defines an broker between event messages and nodes
type Actor interface {

	// ID returns the unique ID for the actor
	ID() string

	// Types returns the underlying types the actor subscribes to
	Types() []string

	// Write pushes information to the actor
	Write([]byte) error

	// Close the actor
	Close()

	// NoForward decides if messages should be forwarded to the actor
	NoForward() bool

	// Done returns if the actor is done messaging.
	Done() bool
}

// ActorGroup holds a group of actors
type ActorGroup interface {

	// Add an actor to a group
	Add(a Actor)

	// Prune removes any done actors
	Prune() bool

	// Walk over the actors with in the group one by one (order is not
	// guaranteed).
	Walk(func(Actor) error) error
}

// EventBroadcaster defines a way to broadcast events to other internal parts
// of the system.
type EventBroadcaster interface {

	// Dispatch an event to other parts of the underlying system
	Dispatch(map[string]interface{}) error
}

// SchedulerTask defines a task that can be run repeatedly
type SchedulerTask interface {

	// Run setups up the given schedule
	Run() (task.Func, task.Schedule)
}

type httpRouteHandler interface {
	http.Handler

	// Add a service to a router
	Add(string, Service)
}

func makeRestServer(
	routerFn func(*mux.Router) httpRouteHandler,
	facade Facade,
	services, internalServices []Service,
	handlerFns map[string]http.HandlerFunc,
	opts *options,
) *restServer {
	mux := mux.NewRouter()
	mux.StrictSlash(false)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		SyncResponse(true, []string{"/1.0"}).Render(w)
	})

	for endpoint, f := range handlerFns {
		mux.HandleFunc(endpoint, f)
	}

	mux.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		level.Info(opts.logger).Log("msg", "Sending top level 404", "url", r.URL)
		w.Header().Set("Content-Type", "application/json")
		NotFound(nil).Render(w)
	})

	router := routerFn(mux)

	for _, service := range opts.services {
		router.Add("1.0", service)
	}
	for _, service := range opts.internalServices {
		router.Add("internal", service)
	}

	return &restServer{
		router: router,
		facade: facade,
	}
}

type restServer struct {
	router http.Handler
	facade Facade

	mutex  sync.Mutex
	config *clusterconfig.ReadOnlyConfig

	logger log.Logger
}

func (s *restServer) Init(config *clusterconfig.ReadOnlyConfig) {
	s.config = config
}

func (s *restServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers, unless this is an internal or gRPC request.
	if !strings.HasPrefix(r.URL.Path, "/internal") && !strings.HasPrefix(r.URL.Path, "/protocol.SQL") {
		// Block until we're setup
		select {
		case <-s.facade.SetupChan():
		}

		if s.config == nil {
			InternalError(errors.Errorf("no valid configuration"))
			return
		}

		setCORSHeaders(w, r, s.config)
	}

	// OPTIONS request don't need any further processing
	if r.Method == "OPTIONS" {
		return
	}

	// Call the original server
	s.router.ServeHTTP(w, r)
}

func (s *restServer) setConfig(config *clusterconfig.ReadOnlyConfig) {
	s.mutex.Lock()
	s.config = config
	s.mutex.Unlock()
}

func (s *restServer) getConfig() *clusterconfig.ReadOnlyConfig {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.config
}

func setCORSHeaders(rw http.ResponseWriter, req *http.Request, config *clusterconfig.ReadOnlyConfig) {
	allowedOrigin, err := config.HTTPSAllowedOrigin()
	origin := req.Header.Get("Origin")
	if err != nil && allowedOrigin != "" && origin != "" {
		rw.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
	}

	allowedMethods, err := config.HTTPSAllowedMethods()
	if err != nil && allowedMethods != "" && origin != "" {
		rw.Header().Set("Access-Control-Allow-Methods", allowedMethods)
	}

	allowedHeaders, err := config.HTTPSAllowedHeaders()
	if err != nil && allowedHeaders != "" && origin != "" {
		rw.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
	}

	allowedCredentials, err := config.HTTPSAllowedCredentials()
	if err != nil && allowedCredentials {
		rw.Header().Set("Access-Control-Allow-Credentials", "true")
	}
}

type restServerScheduler struct {
	server *restServer
	daemon Daemon
	mutex  sync.Mutex
	logger log.Logger
}

func makeRestServerScheduler(server *restServer, daemon Daemon, logger log.Logger) *restServerScheduler {
	return &restServerScheduler{
		server: server,
		daemon: daemon,
		logger: logger,
	}
}

// Run returns a task function that performs collecting the configuration from
// the cluster at a given interval.
func (s *restServerScheduler) Run() (task.Func, task.Schedule) {
	// Since the database APIs are blocking we need to wrap the core logic
	// and run it in a goroutine, so we can abort as soon as the context expires.
	restServerWrapper := func(ctx context.Context) {
		ch := make(chan struct{})
		go func() {
			select {
			case <-s.daemon.SetupChan():
			}
			s.run(ctx)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}

	schedule := task.Every(time.Duration(Interval) * time.Second)
	return restServerWrapper, schedule
}

func (s *restServerScheduler) run(ctx context.Context) {
	// clear the config, so that the next read, gets a fresh load.
	s.server.setConfig(nil)

	if _, err := s.readConfig(); err != nil {
		level.Info(s.logger).Log("msg", "error getting config", "err", err)
	}
}

func (s *restServerScheduler) readConfig() (*clusterconfig.ReadOnlyConfig, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if config := s.server.getConfig(); config != nil {
		return config, nil
	}

	var config *clusterconfig.ReadOnlyConfig
	if err := s.daemon.Cluster().Transaction(func(tx *db.ClusterTx) error {
		cfg, err := clusterconfig.Load(tx, s.daemon.ClusterConfigSchema())
		if err != nil {
			return errors.WithStack(err)
		}
		config = cfg.ReadOnly()
		s.server.setConfig(config)

		return nil
	}); err != nil {
		return nil, errors.WithStack(err)
	}

	return config, nil
}

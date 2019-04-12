package daemon

import (
	"context"
	"crypto/x509"
	"database/sql/driver"
	"fmt"
	"net/http"
	"os/user"
	"path/filepath"
	"time"

	"github.com/pborman/uuid"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster"
	clusterevents "github.com/spoke-d/thermionic/internal/cluster/events"
	"github.com/spoke-d/thermionic/internal/cluster/heartbeat"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/upgraded"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	querycluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/discovery"
	"github.com/spoke-d/thermionic/internal/endpoints"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/operations"
	"github.com/spoke-d/thermionic/internal/retrier"
	"github.com/spoke-d/thermionic/internal/schedules"
	"github.com/spoke-d/thermionic/internal/state"
	"github.com/spoke-d/task"
	"github.com/spoke-d/thermionic/pkg/api"
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

	// LocalNoncePath returns the path of the local nonce file.
	LocalNoncePath() string

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

// Node mediates access to the data stored in the node-local SQLite database.
type Node interface {
	database.DBAccessor
	db.NodeOpener
	db.NodeTransactioner

	// Dir returns the directory of the underlying database file.
	Dir() string

	// Close the database facade.
	Close() error
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

// Gateway mediates access to the dqlite cluster using a gRPC SQL client, and
// possibly runs a dqlite replica on this node (if we're configured to do
// so).
type Gateway interface {

	// Init the gateway, creating a new raft factory and server (if this node is a
	// database node), and a dialer.
	Init(*cert.Info) error

	// RaftNodes returns information about the nodes that a currently part of
	// the raft cluster, as configured in the raft log. It returns an error if this
	// node is not the leader.
	RaftNodes() ([]db.RaftNode, error)

	// Raft returns the raft instance
	Raft() RaftInstance

	// DB returns the database node of the cluster
	DB() Node

	// IsDatabaseNode returns true if this gateway also run acts a raft database
	// node.
	IsDatabaseNode() bool

	// Cert returns the currently available cert in the gateway
	Cert() *cert.Info

	// LeaderAddress returns the address of the current raft leader.
	LeaderAddress() (string, error)

	// Reset the gateway, shutting it down and starting against from scratch
	// using the given certificate.
	//
	// This is used when disabling clustering on a node.
	Reset(cert *cert.Info) error

	// Clustered returns if the Gateway is a raft node or is not clustered
	Clustered() bool

	// WaitLeadership will wait for the raft node to become leader. Should only
	// be used by Bootstrap, since we know that we'll self elect.
	WaitLeadership() error

	// WaitUpgradeNotification waits for a notification from another node that all
	// nodes in the cluster should now have been upgraded and have matching schema
	// and API versions.
	WaitUpgradeNotification()

	// HandlerFuncs returns the HTTP handlers that should be added to the REST API
	// endpoint in order to handle database-related requests.
	//
	// There are two handlers, one for the /internal/raft endpoint and the other
	// for /internal/db, which handle respectively raft and gRPC-SQL requests.
	//
	// These handlers might return 404, either because this node is a
	// non-clustered node not available over the network or because it is not a
	// database node part of the dqlite cluster.
	HandlerFuncs() map[string]http.HandlerFunc

	// ServerStore returns a dqlite server store that can be used to lookup the
	// addresses of known database nodes.
	ServerStore() querycluster.ServerStore

	// DialFunc returns a dial function that can be used to connect to one of the
	// dqlite nodes.
	DialFunc() dqlite.DialFunc

	// Context returns a cancellation context to pass to dqlite.NewDriver as
	// option.
	//
	// This context gets cancelled by Gateway.Kill() and at that point any
	// connection failure won't be retried.
	Context() context.Context

	// Kill is an API that the daemon calls before it actually shuts down and calls
	// Shutdown(). It will abort any ongoing or new attempt to establish a SQL gRPC
	// connection with the dialer (typically for running some pre-shutdown
	// queries).
	Kill()

	// Shutdown this gateway, stopping the gRPC server and possibly the raft factory.
	Shutdown() error
}

// RaftInstance is a specific wrapper around raft.Raft, which also holds a
// reference to its network transport and dqlite FSM.
type RaftInstance interface {

	// MembershipChanger returns the underlying rafthttp.Layer, which can be
	// used to change the membership of this node in the cluster.
	MembershipChanger() raftmembership.Changer
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

// Service represents a endpoint that can perform http actions upon
type Service interface {

	// Get handles GET requests
	Get(Daemon, *http.Request) Response

	// Put handles PUT requests
	Put(Daemon, *http.Request) Response

	// Post handles POST requests
	Post(Daemon, *http.Request) Response

	// Delete handles DELETE requests
	Delete(Daemon, *http.Request) Response

	// Patch handles PATCH requests
	Patch(Daemon, *http.Request) Response

	// Name returns the serialisable service name.
	// The name has to conform to RFC 3986
	Name() string
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

// Schedules defines an interface for interacting with a series of tasks
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

// SchedulerTask defines a task that can be run repeatedly
type SchedulerTask interface {

	// Run setups up the given schedule
	Run() (task.Func, task.Schedule)
}

// Response defines a return value from a http request. The response then can
// be rendered.
type Response interface {

	// Render the response with a response writer.
	Render(http.ResponseWriter) error
}

// EventBroadcaster defines a way to broadcast events to other internal parts
// of the system.
type EventBroadcaster interface {

	// Dispatch an event to other parts of the underlying system
	Dispatch(map[string]interface{}) error
}

// Daemon can respond to requests from a shared client.
type Daemon struct {
	clientCerts                           []x509.Certificate
	clusterConfigSchema, nodeConfigSchema config.Schema
	version                               string
	nonce                                 string
	apiExtensions                         []string
	db                                    Node
	cluster                               Cluster
	gateway                               Gateway
	operations                            Operations
	schedules                             Schedules
	os                                    OS

	networkAddress string
	debugAddress   string

	// Tasks registry for long-running background tasks.
	tasks *task.Group

	setupChan    chan struct{}
	registerChan chan struct{}
	shutdownChan chan struct{}

	endpoints        Endpoints
	actorGroup       ActorGroup
	eventBroadcaster EventBroadcaster

	fileSystem fsys.FileSystem
	logger     log.Logger
	sleeper    clock.Sleeper

	raftLatency float64

	apiServices         []api.Service
	apiInternalServices []api.Service
}

// New creates a Daemon with sane defaults
func New(
	clientCerts []x509.Certificate,
	version string,
	networkAddress, debugAddress string,
	clusterConfigSchema, nodeConfigSchema config.Schema,
	apiExtensions []string,
	apiServices, apiInternalServices []api.Service,
	actorGroup ActorGroup,
	eventBroadcaster EventBroadcaster,
	options ...Option,
) *Daemon {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Daemon{
		clientCerts:         clientCerts,
		version:             version,
		networkAddress:      networkAddress,
		debugAddress:        debugAddress,
		apiExtensions:       apiExtensions,
		clusterConfigSchema: clusterConfigSchema,
		nodeConfigSchema:    nodeConfigSchema,
		tasks:               task.NewGroup(),
		setupChan:           make(chan struct{}),
		registerChan:        make(chan struct{}),
		shutdownChan:        make(chan struct{}),
		apiServices:         apiServices,
		apiInternalServices: apiInternalServices,
		actorGroup:          actorGroup,
		eventBroadcaster:    eventBroadcaster,
		os:                  opts.os,
		fileSystem:          opts.fileSystem,
		logger:              opts.logger,
		sleeper:             opts.sleeper,
		raftLatency:         opts.raftLatency,
	}
}

// Init the Daemon, creating all the required dependencies.
func (d *Daemon) Init() error {
	if err := d.init(); err != nil {
		level.Error(d.logger).Log("msg", "failed to start the daemon", "err", err)
		d.Stop()
		return errors.WithStack(err)
	}
	return nil
}

// Register tasks or system services for the daemon
func (d *Daemon) Register(tasks []SchedulerTask) error {
	// register heartbeat task
	heartbeatTask := heartbeat.New(
		makeHeartbeatGatewayShim(d.gateway),
		d.cluster,
		heartbeat.DatabaseEndpoint,
		heartbeat.WithLogger(log.WithPrefix(d.logger, "component", "heartbeat")),
	)
	d.tasks.Add(heartbeatTask.Run())

	// register events task
	eventsTask := clusterevents.New(
		d.endpoints,
		d.cluster,
		NewEventHook(d.eventBroadcaster, d.logger),
		clusterevents.WithLogger(log.WithPrefix(d.logger, "component", "events")),
	)
	d.tasks.Add(eventsTask.Run())

	// register operations task
	operationsTask := operations.New(
		d.cluster,
		operations.WithLogger(log.WithPrefix(d.logger, "component", "operations")),
	)
	d.tasks.Add(operationsTask.Run())
	d.operations = operationsTask

	// register schedule tasks
	schedulesTask := schedules.New(
		makeDaemonSchedulesShim(d),
		schedules.WithLogger(log.WithPrefix(d.logger, "component", "schedules")),
	)
	d.tasks.Add(schedulesTask.Run())
	d.schedules = schedulesTask

	discoveryTask := discovery.New(
		makeDaemonDiscoveryShim(d),
		discovery.WithLogger(log.WithPrefix(d.logger, "component", "discovery")),
	)
	d.tasks.Add(discoveryTask.Run())

	for _, task := range tasks {
		d.tasks.Add(task.Run())
	}

	d.tasks.Start()

	level.Info(d.logger).Log("msg", "registered tasks")
	close(d.registerChan)

	return nil
}

// Kill signals the daemon that we want to shutdown, and that any work
// initiated from this point (e.g. database queries over gRPC) should not be
// retried in case of failure.
func (d *Daemon) Kill() {
	if d.gateway != nil {
		d.gateway.Kill()
	}
}

// Stop stops the shared daemon.
func (d *Daemon) Stop() error {
	level.Info(d.logger).Log("msg", "starting shutdown sequence")

	// Track all the errors, if there is an error continue stopping.
	var errs []error
	trackError := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if d.endpoints != nil {
		trackError(d.endpoints.Down())
	}

	// Give tasks a bit of time to cleanup.
	trackError(d.tasks.Stop(3 * time.Second))

	if d.cluster != nil {
		level.Info(d.logger).Log("msg", "closing the database")
		if err := d.cluster.Close(); errors.Cause(err) == driver.ErrBadConn {
			level.Debug(d.logger).Log("msg", "could not close remote database", "err", err)
		} else {
			trackError(err)
		}
	}

	if d.db != nil {
		trackError(d.db.Close())
	}
	if d.gateway != nil {
		trackError(d.gateway.Shutdown())
	}
	if d.endpoints != nil {
		trackError(d.endpoints.Down())
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
		level.Error(d.logger).Log("msg", "failed to cleanly shutdown", "err", err)
	}
	return errors.WithStack(err)
}

// Gateway returns the underlying Daemon Gateway
func (d *Daemon) Gateway() Gateway {
	return d.gateway
}

// Cluster returns the underlying Cluster
func (d *Daemon) Cluster() Cluster {
	return d.cluster
}

// Node returns the underlying node database
func (d *Daemon) Node() Node {
	return d.db
}

// ClientCerts returns the certificates used locally
func (d *Daemon) ClientCerts() []x509.Certificate {
	return d.clientCerts
}

// ClusterCerts returns the network certificates used for the cluster.
func (d *Daemon) ClusterCerts() []x509.Certificate {
	networkCert := d.endpoints.NetworkCert()
	keyPair := networkCert.KeyPair()
	certs := keyPair.Certificate
	if len(certs) == 0 {
		return make([]x509.Certificate, 0)
	}

	cert, _ := x509.ParseCertificate(certs[0])
	return []x509.Certificate{
		*cert,
	}
}

// ClusterConfigSchema returns the daemon schema for the Cluster
func (d *Daemon) ClusterConfigSchema() config.Schema {
	return d.clusterConfigSchema
}

// NodeConfigSchema returns the daemon schema for the local Node
func (d *Daemon) NodeConfigSchema() config.Schema {
	return d.nodeConfigSchema
}

// ActorGroup returns a group of actors for event processing
func (d *Daemon) ActorGroup() ActorGroup {
	return d.actorGroup
}

// EventBroadcaster returns a event broadcaster for event processing
func (d *Daemon) EventBroadcaster() EventBroadcaster {
	return d.eventBroadcaster
}

// SetupChan returns a channel that blocks until setup has happened from
// the Daemon
func (d *Daemon) SetupChan() <-chan struct{} {
	return d.setupChan
}

// RegisterChan returns a channel that blocks until all the registered tasks
// have happened for the Daemon
func (d *Daemon) RegisterChan() <-chan struct{} {
	return d.registerChan
}

// ShutdownChan returns a channel that blocks until shutdown has happened from
// the Daemon.
func (d *Daemon) ShutdownChan() <-chan struct{} {
	return d.shutdownChan
}

// State creates a new State instance liked to our internal db and os.
func (d *Daemon) State() *state.State {
	return state.NewState(
		state.WithNode(d.db),
		state.WithCluster(d.cluster),
		state.WithOS(d.os),
	)
}

// Version returns the current version of the daemon
func (d *Daemon) Version() string {
	return d.version
}

// Endpoints returns the underlying endpoints that the daemon controls.
func (d *Daemon) Endpoints() Endpoints {
	return d.endpoints
}

// APIExtensions returns the extensions available to the current daemon
func (d *Daemon) APIExtensions() []string {
	return d.apiExtensions
}

// Operations return the underlying operational tasks associated with the
// current daemon
func (d *Daemon) Operations() Operations {
	return d.operations
}

// Schedules return the underlying schedule tasks associated with the
// current daemon
func (d *Daemon) Schedules() Schedules {
	return d.schedules
}

// Nonce returns the nonce for the daemon
func (d *Daemon) Nonce() string {
	return d.nonce
}

func (d *Daemon) init() error {
	level.Info(d.logger).Log("msg", "starting daemon", "version", d.version)

	// initialize the operating system facade
	if err := d.os.Init(d.fileSystem); err != nil {
		return errors.WithStack(err)
	}
	// initialize the nonce
	nonce, err := d.initNonce()
	if err != nil {
		return errors.WithStack(err)
	}
	d.nonce = nonce

	// initialize the database
	if err := d.initDatabase(); err != nil {
		return errors.WithStack(err)
	}
	// load server certificate
	certInfo, err := cert.LoadCert(
		d.os.VarDir(),
		cert.WithFileSystem(d.fileSystem),
		cert.WithOS(d.os),
		cert.WithLogger(log.WithPrefix(d.logger, "component", "cert")),
	)
	if err != nil {
		return errors.WithStack(err)
	}

	gateway := cluster.NewGateway(
		d.db,
		d.nodeConfigSchema,
		d.fileSystem,
		cluster.WithLogger(log.WithPrefix(d.logger, "component", "gateway")),
		cluster.WithLatency(d.raftLatency),
	)
	d.gateway = makeDaemonGatewayShim(gateway)
	if err := d.gateway.Init(certInfo); err != nil {
		return errors.WithStack(err)
	}

	address, err := networkAddress(d.db, d.networkAddress, d.nodeConfigSchema)
	if err != nil {
		return errors.Wrap(err, "failed to fetch node address")
	}

	debugAddress, err := debugAddress(d.db, d.debugAddress, d.nodeConfigSchema)
	if err != nil {
		return errors.Wrap(err, "failed to fetch debug address")
	}

	restServer, configSchedulerTask, err := api.DaemonRestServer(
		makeDaemonShim(d),
		d.apiServices,
		d.apiInternalServices,
		api.WithLogger(log.WithPrefix(d.logger, "component", "api")),
	)
	if err != nil {
		return errors.WithStack(err)
	}

	d.endpoints = endpoints.New(
		restServer,
		certInfo,
		endpoints.WithNetworkAddress(address),
		endpoints.WithDebugAddress(debugAddress),
		endpoints.WithLogger(log.WithPrefix(d.logger, "component", "endpoints")),
	)
	if err := d.endpoints.Up(); err != nil {
		return errors.Wrap(err, "failed to setup API endpoints")
	}

	// open the cluster database
	if err := d.initClusterDatabase(address, certInfo); err != nil {
		return errors.WithStack(err)
	}

	close(d.setupChan)

	return d.Register([]SchedulerTask{
		configSchedulerTask,
	})
}

func (d *Daemon) initNonce() (string, error) {
	level.Debug(d.logger).Log("msg", "initializing local nonce")

	path := d.os.LocalNoncePath()
	if d.fileSystem.Exists(path) {
		if err := d.fileSystem.Remove(path); err != nil {
			return "", errors.WithStack(err)
		}
	}

	nonce := uuid.NewRandom().String()
	file, err := d.fileSystem.Create(path)
	if err != nil {
		return "", errors.WithStack(err)
	}
	if _, err := file.Write([]byte(nonce + "\n")); err != nil {
		return "", errors.WithStack(err)
	}
	if err := file.Sync(); err != nil {
		return "", errors.WithStack(err)
	}
	return nonce, nil
}

func (d *Daemon) initDatabase() error {
	level.Info(d.logger).Log("msg", "initializing local database")

	// Hook to run when the local database is created from scratch. It will
	// create the default profile and mark all patches as applied.
	freshHook := func(*db.Node) error {
		return nil
	}

	d.db = db.NewNode(d.fileSystem)
	if err := d.db.Open(filepath.Join(d.os.VarDir(), "database"), freshHook); err != nil {
		return errors.Wrap(err, "error creating database")
	}
	return nil
}

func (d *Daemon) initClusterDatabase(address string, certInfo *cert.Info) error {
	retry := retrier.New(d.sleeper, 10, time.Millisecond*100)
	if err := retry.Run(func() error {
		level.Info(d.logger).Log("msg", "initializing global database")

		dir := filepath.Join(d.os.VarDir(), "database")
		store := d.gateway.ServerStore()

		queryCluster := querycluster.New(
			querycluster.NewBasicAPIExtensions(len(d.apiExtensions)),
			querycluster.NewSchema(d.fileSystem),
		)

		d.cluster = db.NewCluster(
			queryCluster,
			db.WithFileSystemForCluster(d.fileSystem),
			db.WithLoggerForCluster(log.WithPrefix(d.logger, "component", "cluster")),
		)
		err := d.cluster.Open(
			store, address, dir, 1*time.Minute,
			dqlite.WithDialFunc(d.gateway.DialFunc()),
			dqlite.WithContext(d.gateway.Context()),
			dqlite.WithConnectionTimeout(10*time.Second),
			dqlite.WithLogFunc(cluster.DqliteLog(d.logger)),
		)
		if err == nil {
			return nil
		}

		// If some other nodes have schema or API versions less recent
		// than this node, we block until we receive a notification
		// from the last node being upgraded that everything should be
		// now fine, and then retry
		if errors.Cause(err) == db.ErrSomeNodesAreBehind {
			level.Info(d.logger).Log("msg", "wait for other cluster nodes to upgrade their version")

			// The only thing we want to still do on this node is
			// to run the heartbeat task, in case we are the raft
			// leader.
			heartbeatTask := heartbeat.New(
				makeHeartbeatGatewayShim(d.gateway),
				d.cluster,
				heartbeat.DatabaseEndpoint,
				heartbeat.WithLogger(log.WithPrefix(d.logger, "component", "heartbeat")),
			)
			stop, _ := task.Start(heartbeatTask.Run())
			d.gateway.WaitUpgradeNotification()
			stop(time.Second)

			d.cluster.Close()
		}

		return err
	}); err != nil {
		return errors.Wrap(err, "failed to open cluster database")
	}

	upgradedTask := upgraded.New(
		makeUpgradedStateShim(d.State()),
		certInfo,
		d.nodeConfigSchema,
	)
	if err := upgradedTask.Run(); err != nil {
		// Ignore the error, since it's not fatal for this particular
		// node. In most cases it just means that some nodes are
		// offline.
		level.Debug(d.logger).Log("msg", "could not notify all nodes of database upgrade", "err", err)
	}

	// Lastly check that the cluster setup is enabled.
	enabledTask := membership.NewEnabled(
		makeMembershipStateShim(d.State()),
	)
	clustered, err := enabledTask.Run()
	if err != nil {
		return errors.WithStack(err)
	}

	if clustered {
		// TODO (simon): we should update the user agent features
		level.Info(d.logger).Log("msg", "clustering is enabled")
	}
	return nil
}

func networkAddress(db Node, addr string, configSchema config.Schema) (string, error) {
	address, err := node.HTTPSAddress(db, configSchema)
	if err != nil {
		return "", errors.WithStack(err)
	}
	if address == "" {
		address = addr
		if err := node.PatchConfig(db, "core.https_address", address, configSchema); err != nil {
			return "", errors.WithStack(err)
		}
	}
	return address, nil
}

func debugAddress(db Node, addr string, configSchema config.Schema) (string, error) {
	address, err := node.DebugAddress(db, configSchema)
	if err != nil {
		return "", errors.WithStack(err)
	}
	if address == "" {
		address = addr
		if err := node.PatchConfig(db, "core.debug_address", address, configSchema); err != nil {
			return "", errors.WithStack(err)
		}
	}
	return address, nil
}

// UnsafeShutdown forces an automatic shutdown of the Daemon
func (d *Daemon) UnsafeShutdown() {
	d.shutdownChan <- struct{}{}
}

// UnsafeSetCluster forces a cluster onto the daemon.
func (d *Daemon) UnsafeSetCluster(cluster Cluster) {
	// Note: this is not safe in anyway and can be VERY racy.
	d.cluster = cluster
}

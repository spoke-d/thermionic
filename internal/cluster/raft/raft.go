package raft

import (
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft-boltdb"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	rafthttp "github.com/CanonicalLtd/raft-http"
	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// Endpoint API path that gets routed to a rafthttp.Handler for
// joining/leaving the cluster and exchanging raft commands between nodes.
const Endpoint = "/internal/raft"

// DefaultMaxPool describes how large the pool should be.
const DefaultMaxPool = 2

// Node mediates access to the data stored in the node-local SQLite database.
type Node interface {
	db.NodeTransactioner

	// Dir returns the directory of the underlying database file.
	Dir() string
}

// RaftNodeMediator mediates access to a raft node configuration to gain access
// to a raftNode
type RaftNodeMediator interface {
	// DetermineRaftNode figures out what raft node ID and address we have, if any.
	//
	// This decision is based on the values of the core.https_address config key
	// and on the rows in the raft_nodes table, both stored in the node-level
	// SQLite database.
	DetermineRaftNode(node.Tx, config.Schema) (*db.RaftNode, error)
}

// MembershipChangerFunc describes a function that will be called when any
// *raft.Raft membership changes occur.
type MembershipChangerFunc func(*raft.Raft)

// DialerProvider provides a way to create a dialer for a raft cluster
type DialerProvider interface {
	// Dial creates a rafthttp.Dial function that connects over TLS using the given
	// cluster (and optionally CA) certificate both as client and remote
	// certificate.
	Dial(certInfo *cert.Info) (rafthttp.Dial, error)
}

// HTTPProvider provides new Handlers or Layers to be consumed
type HTTPProvider interface {
	// Handler implements an HTTP handler that will look for an Upgrade
	// header in the request to switch the HTTP connection to raw TCP
	// mode, so it can be used as raft.NetworkTransport stream.
	Handler(*stdlog.Logger) *rafthttp.Handler

	// NewLayer returns a new raft stream layer that initiates connections
	// with HTTP and then uses Upgrade to switch them into raw TCP.
	Layer(string, net.Addr, *rafthttp.Handler, rafthttp.Dial, *stdlog.Logger) *rafthttp.Layer
}

// HTTPHandler defines an HTTP handler that will look for an Upgrade
// header in the request to switch the HTTP connection to raw TCP
// mode, so it can be used as raft.NetworkTransport stream.
type HTTPHandler interface {
	// Requests returns a channel of inbound Raft membership change requests
	// received over HTTP. Consumer code is supposed to process this channel by
	// invoking raftmembership.HandleChangeRequests.
	Requests() <-chan *raftmembership.ChangeRequest

	// ServerHTTP upgrades the given HTTP connection to a raw TCP one for
	// use by raft.
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

// NetworkProvider provides new Transports to be consumed
type NetworkProvider interface {

	// Network creates a new network transport with the given config struct
	Network(*raft.NetworkTransportConfig) raft.Transport
}

// AddressResolver defines an handler to resolve TCP addresses.
type AddressResolver interface {

	// Resolve returns an address of TCP end point.
	//
	// If the host in the address parameter is not a literal IP address or
	// the port is not a literal port number, ResolveTCPAddr resolves the
	// address to an address of TCP end point.
	// Otherwise, it parses the address as a pair of literal IP address
	// and port number.
	// The address parameter can use a host name, but this is not
	// recommended, because it will return at most one of the host name's
	// IP addresses.
	Resolve(string) (net.Addr, error)
}

// LogsProvider is used to provide an interface for getting the log store.
type LogsProvider interface {

	// Logs returns a LogStore
	Logs(string, time.Duration) (LogStore, error)
}

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.
type LogStore interface {
	// FirstIndex returns the first index written. 0 for no entries.
	FirstIndex() (uint64, error)

	// LastIndex returns the last index written. 0 for no entries.
	LastIndex() (uint64, error)

	// GetLog gets a log entry at a given index.
	GetLog(uint64, *raft.Log) error

	// StoreLog stores a log entry.
	StoreLog(*raft.Log) error

	// StoreLogs stores multiple log entries.
	StoreLogs([]*raft.Log) error

	// DeleteRange deletes a range of log entries. The range is inclusive.
	DeleteRange(uint64, uint64) error

	// Set stores the key and value
	Set([]byte, []byte) error

	// Get returns the value for key, or an empty byte slice if key was not
	// found.
	Get([]byte) ([]byte, error)

	// SetUint64 sets a key and a uint64 value
	SetUint64([]byte, uint64) error

	// GetUint64 returns the uint64 value for key, or 0 if key was not found.
	GetUint64([]byte) (uint64, error)

	// Close the log store
	Close() error
}

// SnapshotStoreProvider creates a raft snapshot store
type SnapshotStoreProvider interface {

	// Store returns a new shapshot store for a given path
	Store(string) (raft.SnapshotStore, error)
}

// RegistryProvider creates raft consumeables
type RegistryProvider interface {

	// Registry tracks internal data shared by the dqlite Driver and FSM.
	// The parameter is a string identifying the local node.
	Registry(string) *dqlite.Registry

	// FSM creates a new dqlite FSM, suitable to be passed to raft.NewRaft.
	// It will handle replication of the SQLite write-ahead log.
	FSM(*dqlite.Registry) raft.FSM
}

type RaftProvider interface {
	Raft(*raft.Config, raft.FSM, LogStore, raft.SnapshotStore, raft.Transport) (*raft.Raft, error)
}

// A specific wrapper around raft.Raft, which holds a reference to its
// network transport and dqlite FSM
type instance struct {
	layer             raftmembership.Changer
	handler           http.HandlerFunc
	membershipChanger func(*raft.Raft)
	logs              LogStore
	registry          *dqlite.Registry
	fsm               raft.FSM
	raft              *raft.Raft
}

// Raft instance and all its dependencies, to be used as backend for
// the dqlite driver running on this node.
//
// If this node should not serve as dqlite node, nil is returned.
//
// The raft instance will use an in-memory transport if clustering is not
// enabled on this node.
//
// The certInfo parameter should contain the cluster TLS keypair and optional
// CA certificate.
//
// The latency parameter is a coarse grain measure of how fast/reliable network
// links are. This is used to tweak the various timeouts parameters of the raft
// algorithm. See the raft.Config structure for more details. A value of 1.0
// means use the default values from hashicorp's raft package. Values closer to
// 0 reduce the values of the various timeouts (useful when running unit tests
// in-memory).
type Raft struct {
	database         Node
	mediator         RaftNodeMediator
	dialerProvider   DialerProvider
	raftNode         *db.RaftNode
	cert             *cert.Info
	latency          float64
	timeout          time.Duration
	instance         *instance
	nodeConfigSchema config.Schema

	logger                log.Logger
	fileSystem            fsys.FileSystem
	addressResolver       AddressResolver
	httpProvider          HTTPProvider
	networkProvider       NetworkProvider
	logsProvider          LogsProvider
	snapshotStoreProvider SnapshotStoreProvider
	registryProvider      RegistryProvider
	raftProvider          RaftProvider
}

// New creates a new Raft instance with all it's dependencies.
func New(database Node, cert *cert.Info, nodeConfigSchema config.Schema, fileSystem fsys.FileSystem, options ...Option) *Raft {
	opts := newOptions()
	opts.database = database
	opts.cert = cert
	opts.nodeConfigSchema = nodeConfigSchema
	opts.fileSystem = fileSystem
	for _, option := range options {
		option(opts)
	}

	return &Raft{
		database:              opts.database,
		mediator:              opts.mediator,
		dialerProvider:        opts.dialerProvider,
		addressResolver:       opts.addressResolver,
		httpProvider:          opts.httpProvider,
		networkProvider:       opts.networkProvider,
		fileSystem:            opts.fileSystem,
		logsProvider:          opts.logsProvider,
		snapshotStoreProvider: opts.snapshotStoreProvider,
		registryProvider:      opts.registryProvider,
		raftProvider:          opts.raftProvider,
		cert:                  opts.cert,
		latency:               opts.latency,
		timeout:               opts.timeout,
		nodeConfigSchema:      opts.nodeConfigSchema,
		logger:                opts.logger,
	}
}

// Init the raft instance and all its dependencies, to be used as backend for
// the dqlite driver running on this node.
func (r *Raft) Init() error {
	if r.latency <= 0 {
		return errors.Errorf("latency should be positive")
	}

	// Figure out if actually need to act as dqlite node.
	if err := r.database.Transaction(func(tx *db.NodeTx) error {
		var err error
		r.raftNode, err = r.mediator.DetermineRaftNode(tx, r.nodeConfigSchema)
		return errors.WithStack(err)
	}); err != nil {
		return errors.WithStack(err)
	}

	// If we're not part of the dqlite cluster, there's nothing to do.
	if r.raftNode == nil {
		return nil
	}
	level.Info(r.logger).Log(
		"msg", "Starting database node",
		"id", r.raftNode.ID,
		"address", r.raftNode.Address,
	)

	// Initialize a raft instance along with all needed dependencies.
	err := r.init()
	return errors.WithStack(err)
}

func (r *Raft) init() error {
	raftLogger := NewLogger(r.logger)

	// Raft config.
	config := NewConfig(r.latency)
	config.Logger = raftLogger
	config.LocalID = raft.ServerID(strconv.Itoa(int(r.raftNode.ID)))

	// Raft transport
	var (
		transport         raft.Transport
		handler           HTTPHandler
		layer             raftmembership.Changer
		membershipChanger MembershipChangerFunc
		err               error
	)
	if addr := r.raftNode.Address; addr == "" {
		config.StartAsLeader = true
		transport = r.memoryTransport()
	} else {
		dialer, err := r.dialerProvider.Dial(r.cert)
		if err != nil {
			return errors.WithStack(err)
		}

		transport, handler, layer, err = r.networkTransport(raftLogger, dialer)
		if err != nil {
			return errors.WithStack(err)
		}
		membershipChanger = func(raft *raft.Raft) {
			raftmembership.HandleChangeRequests(raft, handler.Requests())
		}
	}

	if err := raft.ValidateConfig(config); err != nil {
		return errors.Wrap(err, "invalid raft configuration")
	}

	// Data directory
	dir := filepath.Join(r.database.Dir(), "global")
	if !r.fileSystem.Exists(dir) {
		if err := r.fileSystem.Mkdir(dir, 0750); err != nil {
			return errors.WithStack(err)
		}
	}

	// Raft log store
	logs, err := r.logsProvider.Logs(filepath.Join(dir, "logs.db"), r.timeout)
	if err != nil {
		return errors.Wrap(err, "failed to create store for raft logs")
	}

	// Raft snapshot store
	snaps, err := r.snapshotStoreProvider.Store(dir)
	if err != nil {
		return errors.Wrap(err, "failed to create file snapshot store")
	}

	// If we're the initial node, we use the last index persisted in the
	// logs store and other checks to determine if we have ever boostrapped
	// the cluster, and if not we do so.
	if r.raftNode.ID == 1 {
		if err := r.maybeBootstrap(config, logs, snaps, transport); err != nil {
			return errors.Wrap(err, "failed to bootstrap cluster")
		}
	}

	// The dqlite registry and FSM
	registry := r.registryProvider.Registry(dir)
	fsm := r.registryProvider.FSM(registry)

	// The actual raft instance
	raft, err := r.raftProvider.Raft(config, fsm, logs, snaps, transport)
	if err != nil {
		logs.Close()
		return errors.Wrap(err, "failed to start raft")
	}

	if membershipChanger != nil {
		// Process Raft connections over HTTP. This goroutine will
		// terminate when instance.handler.Close() is called, which
		// happens indirectly when the raft instance is shutdown in
		// instance.Shutdown(), and the associated transport is closed.
		go membershipChanger(raft)
	}

	r.instance = &instance{
		layer:             layer,
		handler:           raftHandler(r.cert, handler),
		membershipChanger: membershipChanger,
		logs:              logs,
		registry:          registry,
		fsm:               fsm,
		raft:              raft,
	}

	return nil
}

// Registry returns the Registry associated with the raft instance.
func (r *Raft) Registry() *dqlite.Registry {
	if r.instance == nil {
		return nil
	}
	return r.instance.registry
}

// FSM returns the FSM associated with the raft instance.
func (r *Raft) FSM() raft.FSM {
	if r.instance == nil {
		return nil
	}
	return r.instance.fsm
}

// Raft returns the actual underlying raft instance
func (r *Raft) Raft() *raft.Raft {
	if r.instance == nil {
		return nil
	}
	return r.instance.raft
}

// Servers returns the servers that are currently part of the cluster.
//
// If this raft instance is not the leader, an error is returned.
func (r *Raft) Servers() ([]raft.Server, error) {
	if r.Raft().State() != raft.Leader {
		return nil, raft.ErrNotLeader
	}

	future := r.Raft().GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, errors.WithStack(err)
	}
	configuration := future.Configuration()
	return configuration.Servers, nil
}

// HandlerFunc can be used to handle HTTP requests performed against the
// API RaftEndpoint ("/internal/raft"), in order to join/leave/form the raft
// cluster.
//
// If it returns nil, it means that this node is not supposed to expose a raft
// endpoint over the network, because it's running as a non-clustered single
// node.
func (r *Raft) HandlerFunc() http.HandlerFunc {
	if r.instance == nil || r.instance.handler == nil {
		return nil
	}
	return r.instance.handler.ServeHTTP
}

// MembershipChanger returns the underlying rafthttp.Layer, which can be used
// to change the membership of this node in the cluster.
func (r *Raft) MembershipChanger() raftmembership.Changer {
	if r.instance == nil {
		return nil
	}
	return r.instance.layer
}

// Shutdown raft and any raft-related resource we have instantiated.
func (r *Raft) Shutdown() error {
	level.Info(r.logger).Log("msg", "Stopping raft instance")

	// Invoke raft APIs asynchronously to allow for a timeout.
	timeout := 10 * time.Second

	errCh := make(chan error)
	timer := time.After(timeout)

	go func() {
		errCh <- r.instance.raft.Shutdown().Error()
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return errors.Wrap(err, "failed to shutdown raft")
		}
	case <-timer:
		level.Debug(r.logger).Log("msg", "Timeout triggered waiting for raft to shutdown")
		return errors.Errorf("raft did not shutdown within %s", timeout)
	}

	err := r.instance.logs.Close()
	return errors.Wrap(err, "failed to close logs store")
}

func (r *Raft) memoryTransport() raft.Transport {
	_, transport := raft.NewInmemTransport("0")
	return transport
}

func (r *Raft) networkTransport(logger *stdlog.Logger, dialer rafthttp.Dial) (
	raft.Transport,
	HTTPHandler,
	raftmembership.Changer,
	error,
) {
	addr, err := r.addressResolver.Resolve(r.raftNode.Address)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "invalid node address")
	}
	handler := r.httpProvider.Handler(logger)
	layer := r.httpProvider.Layer(Endpoint, addr, handler, dialer, logger)

	config := &raft.NetworkTransportConfig{
		Logger:  logger,
		Stream:  layer,
		MaxPool: DefaultMaxPool,
		Timeout: r.timeout,
		ServerAddressProvider: &AddressProvider{
			db: r.database,
		},
	}
	transport := r.networkProvider.Network(config)
	return transport, handler, layer, nil
}

// Helper to bootstrap the raft cluster if needed.
func (r *Raft) maybeBootstrap(
	conf *raft.Config,
	logs LogStore,
	snaps raft.SnapshotStore,
	trans raft.Transport,
) error {
	// First check if we were already bootstrapped.
	hasExistingState, err := raft.HasExistingState(logs, logs, snaps)
	if err != nil {
		return errors.Wrap(err, "failed to check if raft has existing state")
	}
	if hasExistingState {
		return nil
	}
	server := raft.Server{
		ID:      conf.LocalID,
		Address: trans.LocalAddr(),
	}
	configuration := raft.Configuration{
		Servers: []raft.Server{
			server,
		},
	}
	return raft.BootstrapCluster(conf, logs, logs, snaps, trans, configuration)
}

func raftHandler(info *cert.Info, handler HTTPHandler) http.HandlerFunc {
	if handler == nil {
		return nil
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if ok, err := cert.TLSCheckCert(r, info); !ok {
			http.Error(w, "403 invalid client certificate", http.StatusForbidden)
			return
		} else if err != nil {
			http.Error(w, "500 internal server error", http.StatusInternalServerError)
			return
		}
		handler.ServeHTTP(w, r)
	}
}

type addressResolver struct{}

func (addressResolver) Resolve(address string) (net.Addr, error) {
	return net.ResolveTCPAddr("tcp", address)
}

type httpProvider struct{}

func (httpProvider) Handler(logger *stdlog.Logger) *rafthttp.Handler {
	return rafthttp.NewHandlerWithLogger(logger)
}

func (httpProvider) Layer(
	path string,
	localAddr net.Addr,
	handler *rafthttp.Handler,
	dial rafthttp.Dial,
	logger *stdlog.Logger,
) *rafthttp.Layer {
	return rafthttp.NewLayerWithLogger(path, localAddr, handler, dial, logger)
}

type networkProvider struct{}

func (networkProvider) Network(config *raft.NetworkTransportConfig) raft.Transport {
	return raft.NewNetworkTransportWithConfig(config)
}

type raftNodeMediatorShim struct{}

func (raftNodeMediatorShim) DetermineRaftNode(tx node.Tx, configSchema config.Schema) (*db.RaftNode, error) {
	return node.DetermineRaftNode(tx, configSchema)
}

type dialerProviderShim struct{}

func (dialerProviderShim) Dial(certInfo *cert.Info) (rafthttp.Dial, error) {
	return NewDialer(certInfo)
}

type logsProvider struct{}

func (logsProvider) Logs(path string, timeout time.Duration) (LogStore, error) {
	return raftboltdb.New(raftboltdb.Options{
		Path: path,
		BoltOptions: &bolt.Options{
			Timeout: timeout,
		},
	})
}

type snapshotStoreProvider struct{}

func (snapshotStoreProvider) Store(path string) (raft.SnapshotStore, error) {
	return raft.NewFileSnapshotStoreWithLogger(path, 2, stdlog.New(ioutil.Discard, "", 0))
}

type registryProvider struct{}

func (registryProvider) Registry(dir string) *dqlite.Registry {
	return dqlite.NewRegistry(dir)
}

func (registryProvider) FSM(registry *dqlite.Registry) raft.FSM {
	return dqlite.NewFSM(registry)
}

type raftProvider struct{}

func (raftProvider) Raft(
	conf *raft.Config,
	fsm raft.FSM,
	logs LogStore,
	snaps raft.SnapshotStore,
	transport raft.Transport,
) (*raft.Raft, error) {
	return raft.NewRaft(conf, fsm, logs, logs, snaps, transport)
}

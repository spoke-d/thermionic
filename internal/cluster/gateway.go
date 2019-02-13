package cluster

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster/heartbeat"
	"github.com/spoke-d/thermionic/internal/cluster/raft"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/internal/retrier"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	hashiraft "github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

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

// RaftProvider allows the creation of new raft instances
type RaftProvider interface {

	// New creates a new raft instance to be used as a backend for the dqlite
	// driver running on the node
	New(Node, *cert.Info, config.Schema, fsys.FileSystem, log.Logger, float64) RaftInstance
}

// RaftInstance and all its dependencies, to be used as backend for
// the dqlite driver running on this node.
type RaftInstance interface {

	// Init the raft instance and all its dependencies, to be used as backend for
	// the dqlite driver running on this node.
	Init() error

	// HandlerFunc can be used to handle HTTP requests performed against the
	// API RaftEndpoint ("/internal/raft"), in order to join/leave/form the raft
	// cluster.
	//
	// If it returns nil, it means that this node is not supposed to expose a raft
	// endpoint over the network, because it's running as a non-clustered single
	// node.
	HandlerFunc() http.HandlerFunc

	// Raft returns the actual underlying raft instance
	Raft() *hashiraft.Raft

	// Registry returns the Registry associated with the raft instance.
	Registry() *dqlite.Registry

	// Servers returns the servers that are currently part of the cluster.
	//
	// If this raft instance is not the leader, an error is returned.
	Servers() ([]hashiraft.Server, error)

	// Shutdown raft and any raft-related resource we have instantiated.
	Shutdown() error

	// MembershipChanger returns the underlying rafthttp.Layer, which can be used
	// to change the membership of this node in the cluster.
	MembershipChanger() raftmembership.Changer
}

// ServerProvider creates a new Server instance.
type ServerProvider interface {

	// New creates a new Server instance.
	New(RaftInstance, net.Listener, *raft.AddressProvider, log.Logger) (Server, error)
}

// Server implements the dqlite network protocol.
type Server interface {
	// Dump the files of a database to disk.
	Dump(string, string) error

	// Close the server, releasing all resources it created.
	Close() error
}

// Net provides functions for acessing unix connections or sockets
type Net interface {
	// UnixListen returns a net.Listener based on the address
	// UnixListen announces on the local network address.
	UnixListen(string) (net.Listener, error)

	// UnixDial returns a net.Conn based on the address
	// UnixDial connects to the address on the named network.
	UnixDial(string) (net.Conn, error)
}

// StoreProvider provides a way to create different store types depending on
// the requirements of the gateway
type StoreProvider interface {

	// Memory creates ServerStore which stores its data in-memory.
	Memory() cluster.ServerStore

	// Disk is used by a dqlite client to get an initial list of candidate
	// dqlite server addresses that it can dial in order to find a leader dqlite
	// server to use.
	Disk(database.DB) (cluster.ServerStore, error)
}

// Gateway mediates access to the dqlite cluster using a gRPC SQL client, and
// possibly runs a dqlite replica on this node (if we're configured to do
// so).
type Gateway struct {
	database         Node
	fileSystem       fsys.FileSystem
	cert             *cert.Info
	nodeConfigSchema config.Schema
	raft             RaftInstance

	// The gRPC server exposing the dqlite driver created by this
	// gateway. It's nil if this node is not supposed to be part of the
	// raft cluster.
	server   Server
	acceptCh chan net.Conn

	// A dialer that will connect to the dqlite server using a loopback
	// net.Conn. It's non-nil when clustering is not enabled on this
	// node, and so we don't expose any dqlite or raft network endpoint,
	// but still we want to use dqlite as backend for the "cluster"
	// database, to minimize the difference between code paths in
	// clustering and non-clustering modes.
	memoryDial dqlite.DialFunc

	// Used when shutting down the daemon to cancel any ongoing gRPC
	// dialing attempt.
	ctx    context.Context
	cancel context.CancelFunc

	// Used to unblock nodes that are waiting for other nodes to upgrade
	// their version.
	upgradeCh chan struct{}

	// ServerStore wrapper.
	store *ServerStore

	// Providers
	raftProvider    RaftProvider
	netProvider     Net
	storeProvider   StoreProvider
	serverProvider  ServerProvider
	sleeper         clock.Sleeper
	addressProvider *raft.AddressProvider

	latency float64
	logger  log.Logger
}

// NewGateway creates a new Gateway for managing access to the dqlite cluster.
//
// When a new gateway is created, the node-level database is queried to check
// what kind of role this node plays and if it's exposed over the network. It
// will initialize internal data structures accordingly, for example starting a
// dqlite driver if this node is a database node.
//
// After creation, the Daemon is expected to expose whatever http handlers the
// HandlerFuncs method returns and to access the dqlite cluster using the gRPC
// dialer returned by the Dialer method.
func NewGateway(database Node, nodeConfigSchema config.Schema, fileSystem fsys.FileSystem, options ...Option) *Gateway {
	opts := newOptions()
	opts.database = database
	opts.nodeConfigSchema = nodeConfigSchema
	opts.fileSystem = fileSystem
	for _, option := range options {
		option(opts)
	}

	ctx, cancel := context.WithCancel(context.Background())

	gateway := &Gateway{
		database:         opts.database,
		cert:             opts.cert,
		nodeConfigSchema: opts.nodeConfigSchema,
		fileSystem:       opts.fileSystem,
		ctx:              ctx,
		cancel:           cancel,
		upgradeCh:        make(chan struct{}, 16),
		acceptCh:         make(chan net.Conn),
		store:            &ServerStore{},

		raftProvider:   opts.raftProvider,
		netProvider:    opts.netProvider,
		storeProvider:  opts.storeProvider,
		serverProvider: opts.serverProvider,
		sleeper:        opts.sleeper,
		latency:        opts.latency,
		logger:         opts.logger,
	}

	return gateway
}

// Init the gateway, creating a new raft factory and server (if this node is a
// database node), and a dialer.
func (g *Gateway) Init(cert *cert.Info) error {
	level.Info(g.logger).Log("msg", "initializing database gateway")
	g.cert = cert

	raftInst := g.raftProvider.New(
		g.database,
		g.cert,
		g.nodeConfigSchema,
		g.fileSystem,
		g.logger,
		g.latency,
	)
	if err := raftInst.Init(); err != nil {
		return errors.Wrap(err, "failed to create raft factory")
	}

	// If the resulting raft instance is not nil, it means that this node
	// should serve as database node, so create a dqlite driver to be
	// exposed it over gRPC.
	if raftInst != nil && raftInst.Raft() != nil {
		listener, err := g.netProvider.UnixListen("")
		if err != nil {
			return errors.Wrap(err, "failed to allocate loopback port")
		}

		if raftInst.HandlerFunc() == nil {
			g.memoryDial = dqliteMemoryDial(g.netProvider, listener)
			g.store.inMemory = g.storeProvider.Memory()

			if err := g.store.Set(context.Background(), []cluster.ServerInfo{
				{Address: "0"},
			}); err != nil {
				return errors.Wrap(err, "failed to set address on memory store")
			}
		} else {
			// ensure we run the proxy
			go func(proxy DqliteProxyFn) {
				proxy(listener, g.acceptCh)
			}(DqliteProxy(g.logger, g.netProvider))
			g.store.inMemory = nil
		}

		provider := raft.NewAddressProvider(g.database)
		server, err := g.serverProvider.New(raftInst, listener, provider, g.logger)
		if err != nil {
			return errors.Wrap(err, "failed to create dqlite server")
		}
		g.addressProvider = provider

		g.server = server
		g.raft = raftInst
	} else {
		g.server = nil
		g.raft = nil
		g.store.inMemory = nil
	}

	// ensure we have a backing store.
	store, err := g.storeProvider.Disk(g.database.DB())
	if err != nil {
		return errors.WithStack(err)
	}
	g.store.onDisk = store

	return nil
}

// HandlerFuncs returns the HTTP handlers that should be added to the REST API
// endpoint in order to handle database-related requests.
//
// There are two handlers, one for the /internal/raft endpoint and the other
// for /internal/db, which handle respectively raft and gRPC-SQL requests.
//
// These handlers might return 404, either because this node is a
// non-clustered node not available over the network or because it is not a
// database node part of the dqlite cluster.
func (g *Gateway) HandlerFuncs() map[string]http.HandlerFunc {
	databaseHandler := func(w http.ResponseWriter, r *http.Request) {
		ok, err := cert.TLSCheckCert(r, g.cert)
		if err != nil {
			http.Error(w, "500 server error", http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "403 invalid client certificate", http.StatusForbidden)
			return
		}

		// Handle hearbeats
		if r.Method == "PUT" {
			g.handleHeartbeats(w, r)
			return
		}

		// From here on we require that this node is part of the raft cluster.
		if !g.Clustered() {
			http.NotFound(w, r)
			return
		}

		// Handle database upgrade notifications.
		if r.Method == "PATCH" {
			select {
			case g.upgradeCh <- struct{}{}:
			default:
			}
			return
		}

		// Before actually establishing the gRPC SQL connection, our
		// dialer probes the node to see if it's currently the leader
		// (otherwise it tries with another node or retry later).
		if r.Method == "HEAD" {
			g.handleLeadershipState(w, r)
			return
		}

		// Handle leader address requests.
		if r.Method == "GET" {
			g.handleLeadership(w, r)
			return
		}

		if r.Header.Get("Upgrade") != "dqlite" {
			http.Error(w, "Missing or invalid upgrade header", http.StatusBadRequest)
			return
		}

		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Webserver doesn't support hijacking", http.StatusInternalServerError)
			return
		}

		conn, _, err := hijacker.Hijack()
		if err != nil {
			message := errors.Wrap(err, "Failed to hijack connection").Error()
			http.Error(w, message, http.StatusInternalServerError)
			return
		}

		// Write the status line and upgrade header by hand since w.WriteHeader()
		// would fail after Hijack()
		data := []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: dqlite\r\n\r\n")
		if n, err := conn.Write(data); err != nil || n != len(data) {
			conn.Close()
			return
		}

		g.acceptCh <- conn
	}
	raftHandler := func(w http.ResponseWriter, r *http.Request) {
		// If we are not part of the raft cluster, reply with a
		// redirect to one of the raft nodes that we know about.
		if g.raft == nil {
			var address string
			if err := g.database.Transaction(func(tx *db.NodeTx) error {
				nodes, err := tx.RaftNodes()
				if err != nil {
					return errors.WithStack(err)
				}
				if len(nodes) == 0 {
					return errors.Errorf("no raft nodes found")
				}
				address = nodes[0].Address
				return nil
			}); err != nil {
				http.Error(w, "500 failed to fetch raft nodes", http.StatusInternalServerError)
				return
			}
			url := &url.URL{
				Scheme:   "http",
				Path:     r.URL.Path,
				RawQuery: r.URL.RawQuery,
				Host:     address,
			}
			http.Redirect(w, r, url.String(), http.StatusPermanentRedirect)
			return
		}

		// If this node is not clustered return a 404.
		handlerFunc := g.raft.HandlerFunc()
		if handlerFunc == nil {
			http.NotFound(w, r)
			return
		}

		handlerFunc(w, r)
	}
	return map[string]http.HandlerFunc{
		heartbeat.DatabaseEndpoint: databaseHandler,
		raft.Endpoint:              raftHandler,
	}
}

// WaitLeadership should wait for the raft node to become leader. Should ideally
// only be used by Bootstrap, since we know that we'll self elect.
func (g *Gateway) WaitLeadership() error {
	retry := retrier.New(g.sleeper, 100, 250*time.Millisecond)
	err := retry.Run(func() error {
		if g.raft.Raft().State() == hashiraft.Leader {
			return nil
		}
		return errors.New("node is not leader")
	})
	return errors.WithMessage(err, "Node did not self-elect with timeframe")
}

// IsDatabaseNode returns true if this gateway also run acts a raft database node.
func (g *Gateway) IsDatabaseNode() bool {
	return g.raft != nil
}

// WaitUpgradeNotification waits for a notification from another node that all
// nodes in the cluster should now have been upgraded and have matching schema
// and API versions.
func (g *Gateway) WaitUpgradeNotification() {
	<-g.upgradeCh
}

// DB returns the underlying database node
func (g *Gateway) DB() Node {
	return g.database
}

// Cert returns the cert associated with the node
func (g *Gateway) Cert() *cert.Info {
	return g.cert
}

// Clustered returns if the Gateway is in a cluster or not
func (g *Gateway) Clustered() bool {
	return g.server != nil && g.memoryDial == nil
}

// Raft returns the underlying raft instance
func (g *Gateway) Raft() RaftInstance {
	return g.raft
}

// RaftNodes returns the nodes currently part of the raft cluster.
func (g *Gateway) RaftNodes() ([]db.RaftNode, error) {
	if g.raft == nil {
		return nil, hashiraft.ErrNotLeader
	}
	servers, err := g.raft.Servers()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	nodes := make([]db.RaftNode, len(servers))

	addressProvider := g.addressProvider
	if addressProvider == nil {
		addressProvider = raft.NewAddressProvider(g.database)
	}

	for i, server := range servers {
		address, err := addressProvider.ServerAddr(server.ID)
		if err != nil {
			if err != db.ErrNoSuchObject {
				return nil, errors.Wrap(err, "failed to fetch raft server address")
			}

			// Use the initial address as fallback. This is an edge
			// case that happens when a new leader is elected and
			// its raft_nodes table is not fully up-to-date yet.
			address = server.Address
		}

		id, err := strconv.Atoi(string(server.ID))
		if err != nil {
			return nil, errors.Wrap(err, "non-numeric server id")
		}

		nodes[i].ID = int64(id)
		nodes[i].Address = string(address)
	}
	return nodes, nil
}

// LeaderAddress returns the address of the current raft leader.
func (g *Gateway) LeaderAddress() (string, error) {
	// If we aren't clustered, return an error.
	if g.memoryDial != nil {
		return "", errors.New("node is not clustered")
	}

	ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
	defer cancel()

	// If this is a raft node, return the address of the current leader, or
	// wait a bit until one is elected.
	if g.raft != nil {
		for ctx.Err() == nil {
			if address := string(g.raft.Raft().Leader()); address != "" {
				return address, nil
			}
			time.Sleep(time.Second)
		}
		return "", ctx.Err()
	}

	// If this isn't a raft node, contact a raft node and ask for the address
	// of the current leader.
	config, err := cert.TLSClientConfig(g.cert)
	if err != nil {
		return "", err
	}

	var addresses []string
	if err := g.database.Transaction(func(tx *db.NodeTx) error {
		nodes, err := tx.RaftNodes()
		if err != nil {
			return errors.WithStack(err)
		}
		for _, node := range nodes {
			addresses = append(addresses, node.Address)
		}
		return nil
	}); err != nil {
		return "", errors.Wrap(err, "failed to fetch raft nodes addresses")
	}

	if len(addresses) == 0 {
		// This should never happen because the raft_nodes table should be never
		// empty for a clustered node, but check if for good measure.
		return "", errors.New("no raft node known")
	}

	// Attempt to find the leader address, there is no checking for consistency
	// or split brain.
	for _, address := range addresses {
		url := fmt.Sprintf("https://%s%s", address, heartbeat.DatabaseEndpoint)
		request, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return "", errors.WithStack(err)
		}

		request = request.WithContext(ctx)
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: config,
			},
		}
		response, err := client.Do(request)
		if err != nil {
			level.Debug(g.logger).Log("msg", "failed to fetch leader address", "address", address, "err", err)
			continue
		}

		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			level.Debug(g.logger).Log("msg", "request for leader address failed", "address", address, "status", response.Status)
			continue
		}

		info := map[string]string{}
		if err := json.Read(response.Body, &info); err != nil {
			level.Debug(g.logger).Log("msg", "failed to parse leader address", "address", address, "err", err)
			continue
		}
		leader, ok := info["leader"]
		if !ok || leader == "" {
			level.Debug(g.logger).Log("msg", "raft node return no leader address", "address", address)
			continue
		}

		return leader, nil
	}

	return "", errors.New("raft cluster is unavailable")
}

// Context returns a cancellation context to pass to dqlite.NewDriver as
// option.
//
// This context gets cancelled by Gateway.Kill() and at that point any
// connection failure won't be retried.
func (g *Gateway) Context() context.Context {
	return g.ctx
}

// DialFunc returns a dial function that can be used to connect to one of the
// dqlite nodes.
func (g *Gateway) DialFunc() dqlite.DialFunc {
	return func(ctx context.Context, address string) (net.Conn, error) {
		// Memory connection.
		if g.memoryDial != nil {
			return g.memoryDial(ctx, address)
		}

		return dqliteNetworkDial(ctx, address, g.cert)
	}
}

// ServerStore returns a dqlite server store that can be used to lookup the
// addresses of known database nodes.
func (g *Gateway) ServerStore() cluster.ServerStore {
	return g.store
}

// Shutdown this gateway, stopping the gRPC server and possibly the raft factory.
func (g *Gateway) Shutdown() error {
	level.Info(g.logger).Log("msg", "Stop database gateway")

	if g.raft != nil {
		if err := g.raft.Shutdown(); err != nil {
			return errors.Wrap(err, "failed to shutdown raft")
		}
	}

	if g.server != nil {
		g.Sync()
		g.server.Close()

		// Unset the memory dial, since Shutdown() is also called for
		// switching between in-memory and network mode.
		g.memoryDial = nil
	}

	return nil
}

// Sync dumps the content of the database to disk. This is useful for
// inspection purposes, and it's also needed by the activateifneeded command so
// it can inspect the database in order to decide whether to activate the
// daemon or not.
func (g *Gateway) Sync() {
	if g.server == nil {
		return
	}

	dir := filepath.Join(g.database.Dir(), "global")
	if err := g.server.Dump("db.bin", dir); err != nil {
		// Just log a warning, since this is not fatal.
		level.Warn(g.logger).Log("msg", "Failed to dump database to disk", "err", err)
	}
}

// Kill is an API that the daemon calls before it actually shuts down and calls
// Shutdown(). It will abort any ongoing or new attempt to establish a SQL gRPC
// connection with the dialer (typically for running some pre-shutdown
// queries).
func (g *Gateway) Kill() {
	level.Debug(g.logger).Log("msg", "Cancel ongoing or future connection attempts")
	g.cancel()
}

// Reset the gateway, shutting it down and starting against from scratch using
// the given certificate.
//
// This is used when disabling clustering on a node.
func (g *Gateway) Reset(cert *cert.Info) error {
	if err := g.Shutdown(); err != nil {
		return errors.WithStack(err)
	}
	if err := g.fileSystem.RemoveAll(filepath.Join(g.database.Dir(), "global")); err != nil {
		return errors.WithStack(err)
	}
	if err := g.database.Transaction(func(tx *db.NodeTx) error {
		return tx.RaftNodesReplace(nil)
	}); err != nil {
		return err
	}
	return g.Init(cert)
}

func (g *Gateway) handleHeartbeats(w http.ResponseWriter, r *http.Request) {
	var nodes []db.RaftNode
	if err := json.Read(r.Body, &nodes); err != nil {
		http.Error(w, "400 invalid raft nodes payload", http.StatusBadRequest)
		return
	}
	if err := g.database.Transaction(func(tx *db.NodeTx) error {
		// validate the raft nodes
		current, err := tx.RaftNodes()
		if err != nil {
			return errors.WithStack(err)
		}
		// If nodes match then we can be assured that we do want to check if
		// nodes match.
		if len(current) == len(nodes) {
			new := make(map[string]db.RaftNode, len(current))
			for _, v := range current {
				new[v.Address] = v
			}
			identical := true
			for _, v := range nodes {
				if n, ok := new[v.Address]; !ok || n.ID == v.ID {
					identical = false
					break
				}
			}
			// Nothing to do, we're already at the same quorum
			if identical {
				return nil
			}
		}
		level.Debug(g.logger).Log("msg", fmt.Sprintf("replace current raft nodes with notes %+v", nodes))
		err = tx.RaftNodesReplace(nodes)
		return errors.WithStack(err)
	}); err != nil {
		http.Error(w, "500 failed to update raft nodes", http.StatusInternalServerError)
		return
	}
}

func (g *Gateway) handleLeadershipState(w http.ResponseWriter, r *http.Request) {
	if g.raft.Raft().State() != hashiraft.Leader {
		http.Error(w, "503 not leader", http.StatusServiceUnavailable)
		return
	}
}

func (g *Gateway) handleLeadership(w http.ResponseWriter, r *http.Request) {
	leader, err := g.LeaderAddress()
	if err != nil {
		http.Error(w, "500 no elected leader", http.StatusInternalServerError)
		return
	}
	json.Write(w, map[string]string{
		"leader": leader,
	}, false, g.logger)
}

type raftProvider struct{}

func (raftProvider) New(
	database Node,
	cert *cert.Info,
	nodeConfigSchema config.Schema,
	fileSystem fsys.FileSystem,
	logger log.Logger,
	latency float64,
) RaftInstance {
	return raft.New(
		database,
		cert,
		nodeConfigSchema,
		fileSystem,
		raft.WithLogger(log.WithPrefix(logger, "component", "raft")),
		raft.WithLatency(latency),
	)
}

type netProvider struct{}

func (netProvider) UnixListen(addr string) (net.Listener, error) {
	return net.Listen("unix", addr)
}

func (netProvider) UnixDial(addr string) (net.Conn, error) {
	return net.Dial("unix", addr)
}

type storeProvider struct{}

func (storeProvider) Memory() cluster.ServerStore {
	return makeServerStore(dqlite.NewInmemServerStore())
}

func (storeProvider) Disk(db database.DB) (cluster.ServerStore, error) {
	raw, err := database.RawSQLDatabase(db)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return makeServerStore(dqlite.NewServerStore(raw, "main", "raft_nodes", "address")), nil
}

type serverProvider struct{}

func (serverProvider) New(
	raftInst RaftInstance,
	listener net.Listener,
	provider *raft.AddressProvider,
	logger log.Logger,
) (Server, error) {
	return dqlite.NewServer(
		raftInst.Raft(), raftInst.Registry(), listener,
		dqlite.WithServerAddressProvider(provider),
		dqlite.WithServerLogFunc(DqliteLog(logger)),
	)
}

// ServerStore uses the in-memory or the on-disk server store conditionally
type ServerStore struct {
	inMemory cluster.ServerStore
	onDisk   cluster.ServerStore
}

// Get return the list of known servers.
func (s *ServerStore) Get(ctx context.Context) ([]cluster.ServerInfo, error) {
	if s.inMemory != nil {
		return s.inMemory.Get(ctx)
	}
	return s.onDisk.Get(ctx)
}

// Set updates the list of known cluster servers.
func (s *ServerStore) Set(ctx context.Context, servers []cluster.ServerInfo) error {
	if s.inMemory != nil {
		return s.inMemory.Set(ctx, servers)
	}
	return s.onDisk.Set(ctx, servers)
}

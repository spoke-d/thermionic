package discovery

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/client"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/task"
)

// Gateway mediates access to the dqlite cluster using a gRPC SQL client, and
// possibly runs a dqlite replica on this node (if we're configured to do
// so).
type Gateway interface {

	// IsDatabaseNode returns true if this gateway also run acts a raft database
	// node.
	IsDatabaseNode() bool

	// LeaderAddress returns the address of the current raft leader.
	LeaderAddress() (string, error)
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

	// NetworkPrivateKey returns the private key of the TLS certificate used by the
	// network endpoint.
	NetworkPrivateKey() []byte

	// NetworkPublicKey returns the public key of the TLS certificate used by the
	// network endpoint.
	NetworkPublicKey() []byte
}

// Daemon can respond to requests from a shared client.
type Daemon interface {
	// Gateway returns the underlying Daemon Gateway
	Gateway() Gateway

	// Cluster returns the underlying Cluster
	Cluster() Cluster

	// Node returns the underlying Node associated with the daemon
	Node() Node

	// NodeConfigSchema returns the daemon schema for the local Node
	NodeConfigSchema() config.Schema

	// Endpoints returns the underlying endpoints that the daemon controls.
	Endpoints() Endpoints
}

// Interval represents the number of seconds to wait between each schedule
// iteration
const Interval = 10

// Discovery represents a task collection of things that want to be
// run
type Discovery struct {
	daemon Daemon
	mutex  sync.Mutex
	clock  clock.Clock
	logger log.Logger
}

// New creates a series of tasks to be worked on
func New(daemon Daemon, options ...Option) *Discovery {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Discovery{
		daemon: daemon,
		clock:  opts.clock,
		logger: opts.logger,
	}
}

// Run returns a task function that performs operational GC checks against
// the internal cache of operations.
func (s *Discovery) Run() (task.Func, task.Schedule) {
	operationsWrapper := func(ctx context.Context) {
		ch := make(chan struct{})
		go func() {
			s.run(ctx)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}

	schedule := task.Every(time.Duration(Interval) * time.Second)
	return operationsWrapper, schedule
}

func (s *Discovery) run(ctx context.Context) {
	// verify if we're clustered or not
	clustered, err := cluster.Enabled(s.daemon.Node())
	if err != nil {
		level.Error(s.logger).Log("msg", "cluster enabled", "err", err)
		return
	}
	if !clustered {
		return
	}

	// Redirect all requests to the leader, which is the one with with
	// up-to-date knowledge of what nodes are part of the raft cluster.
	localAddress, err := node.HTTPSAddress(s.daemon.Node(), s.daemon.NodeConfigSchema())
	if err != nil {
		return
	}
	leader, err := s.daemon.Gateway().LeaderAddress()
	if err != nil {
		return
	}
	if localAddress != leader {
		// We're not the leader, so skip any thought of running the scheduler
		return
	}

	var nodes []db.NodeInfo
	var serviceNodes []db.ServiceNodeInfo
	if err := s.daemon.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		if nodes, err = tx.Nodes(); err != nil {
			return errors.WithStack(err)
		}
		if serviceNodes, err = tx.ServiceNodes(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		level.Error(s.logger).Log("msg", "attempting to retrive nodes", "err", err)
	}

	// work out if the service nodes already exist in the cluster.
	add, remove := Intersection(nodes, serviceNodes)
	// Ensure we remove the leader from any of these.
	if _, ok := add[leader]; ok {
		delete(add, leader)
	}
	if _, ok := remove[leader]; ok {
		delete(remove, leader)
	}

	// validate the length
	if len(add) == 0 && len(remove) == 0 {
		level.Debug(s.logger).Log("msg", "nothing to add/remove")
		return
	}

	endpoints := s.daemon.Endpoints()
	cert := string(endpoints.NetworkPublicKey())
	key := string(endpoints.NetworkPrivateKey())

	// from here on, we assume that we can talk to the node, as they should
	// have the same certs. If not this isn't going to work.
	for _, v := range add {
		// use an untrusted client to get the daemon certs
		c, err := client.NewUntrusted(
			v.DaemonAddress,
			v.DaemonNonce,
			client.WithLogger(log.WithPrefix(s.logger, "component", "discovery-client", "action", "join")),
		)
		if err != nil {
			level.Error(s.logger).Log("msg", "unable to construct untrusted client", "err", err)
			continue
		}

		info, err := c.Info().Get()
		if err != nil {
			level.Error(s.logger).Log("msg", "error requesting daemon information", "err", err)
			continue
		}

		// Use a trusted client to then join the cluster.
		c, err = client.New(
			v.DaemonAddress,
			info.Environment.Certificate,
			info.Environment.Certificate,
			info.Environment.CertificateKey,
		)
		if err != nil {
			level.Error(s.logger).Log("msg", "unable to construct client", "err", err)
			continue
		}

		nodeInfo := v.NodeInfo
		if err := c.Cluster().Join(nodeInfo.Name, leader, "", client.ClusterCertInfo{
			Certificate: cert,
			Key:         key,
		}); err != nil {
			level.Error(s.logger).Log("msg", "unable to join", "err", err)
			continue
		}
	}

	for _, v := range remove {
		// Use a trusted client to then remove the node from the cluster.
		address := v.DaemonAddress
		if address == "" {
			address = v.NodeInfo.Address
		}
		c, err := client.New(
			address,
			cert,
			cert,
			key,
		)
		if err != nil {
			level.Error(s.logger).Log("msg", "unable to construct client", "err", err)
			continue
		}

		nodeInfo := v.NodeInfo
		if err := c.Cluster().Remove(nodeInfo.Name, true); err != nil {
			level.Error(s.logger).Log("msg", "unable to remove", "err", err)
			continue
		}
	}
}

// NodeData is a tuple containing the NodeInfo and Daemon information.
type NodeData struct {
	NodeInfo      db.NodeInfo
	DaemonAddress string
	DaemonNonce   string
}

type nodeMarker struct {
	Exists bool
	Node   NodeData
}

// Intersection checks to see if there are any nodes and services that
// need to be added or removed.
func Intersection(nodes []db.NodeInfo, services []db.ServiceNodeInfo) (map[string]NodeData, map[string]NodeData) {
	add := make(map[string]nodeMarker)
	remove := make(map[string]NodeData)

	existing := make(map[string]db.NodeInfo, len(nodes))
	for _, v := range nodes {
		existing[v.Address] = v
	}

	// Add all new services as additional nodes, but mark them if they already
	// exist as nodes in the current cluster
	for _, v := range services {
		_, ok := existing[v.DaemonAddress]
		add[v.DaemonAddress] = nodeMarker{
			Exists: ok,
			Node: NodeData{
				NodeInfo: db.NodeInfo{
					Name:      v.Name,
					Address:   v.Address,
					Heartbeat: v.Heartbeat,
				},
				DaemonAddress: v.DaemonAddress,
				DaemonNonce:   v.DaemonNonce,
			},
		}
	}

	// Remove any nodes that don't exist in the add set
	for k, v := range existing {
		if _, ok := add[k]; !ok {
			remove[k] = NodeData{
				NodeInfo: v,
			}
		}
	}

	// Filter out the nodes that already exist, so that we just add nodes that
	// we know are new nodes.
	result := make(map[string]NodeData)
	for k, v := range add {
		if !v.Exists {
			result[k] = v.Node
		}
	}

	return result, remove
}

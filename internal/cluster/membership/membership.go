package membership

import (
	"context"
	"os/user"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/spoke-d/thermionic/internal/cert"
	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	querycluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/node"
)

// We currently aim at having 3 nodes part of the raft dqlite cluster.
const membershipQuorumRaftNodes = 3

// NodeConfigProvider holds node-local configuration values for a certain
// instance.
type NodeConfigProvider interface {
	// ConfigLoad loads a new Config object with the current node-local
	// configuration values fetched from the database. An optional list of
	// config value triggers can be passed, each config key must have at most
	// one trigger.
	ConfigLoad(*db.NodeTx, config.Schema) (*node.Config, error)
}

// ClusterConfigProvider holds cluster configuration values for a certain
// instance.
type ClusterConfigProvider interface {
	// ConfigLoad loads a new Config object with the current node-local
	// configuration values fetched from the database. An optional list of
	// config value triggers can be passed, each config key must have at most
	// one trigger.
	ConfigLoad(*db.ClusterTx, config.Schema) (*clusterconfig.Config, error)
}

// Gateway mediates access to the dqlite cluster using a gRPC SQL client, and
// possibly runs a dqlite replica on this node (if we're configured to do so).
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

	// DB returns the the underlying db node
	DB() Node

	// IsDatabaseNode returns true if this gateway also run acts a raft database
	// node.
	IsDatabaseNode() bool

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

// Node mediates access to the data stored in the node-local SQLite database.
type Node interface {
	db.NodeTransactioner
}

// Cluster mediates access to data stored in the cluster dqlite database.
type Cluster interface {
	db.ClusterOpener
	db.ClusterTransactioner
	db.ClusterExclusiveLocker

	// NodeID sets the the node NodeID associated with this cluster instance.
	// It's used for backward-compatibility of all db-related APIs that were
	// written before clustering and don't accept a node NodeID, so in those
	// cases we automatically use this value as implicit node NodeID.
	NodeID(int64)

	// DB returns the underlying db
	DB() database.DB

	// SchemaVersion returns the underlying schema version for the cluster
	SchemaVersion() int

	// Close the database facade.
	Close() error
}

// Endpoints are in charge of bringing up and down the HTTP endpoints for
// serving the RESTful API.
type Endpoints interface {

	// NetworkCert returns the full TLS certificate information for this
	// endpoint.
	NetworkCert() *cert.Info

	// NetworkUpdateCert updates the cert for the network endpoint,
	// shutting it down and restarting it.
	NetworkUpdateCert(*cert.Info) error
}

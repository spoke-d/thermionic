package upgraded

import (
	"net/http"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/heartbeat"
	"github.com/spoke-d/thermionic/internal/cluster/notifier"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/pkg/client"
	"github.com/pkg/errors"
)

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
	db.ClusterTransactioner
	db.ClusterExclusiveLocker

	// NodeID sets the the node NodeID associated with this cluster instance. It's used for
	// backward-compatibility of all db-related APIs that were written before
	// clustering and don't accept a node NodeID, so in those cases we automatically
	// use this value as implicit node NodeID.
	NodeID(int64)
}

// NotifierProvider creates a new Notifier as a dependency
type NotifierProvider interface {

	// New creates a new Notifier using a cert
	New(State, *cert.Info, config.Schema) Notifier
}

// Notifier is a function that invokes the given function against each node in
// the cluster excluding the invoking one.
type Notifier interface {

	// Run is a function that invokes the given function against each node in
	// the cluster excluding the invoking one.
	Run(func(*client.Client) error, notifier.Policy) error
}

// Upgraded defines a task, that when run notifies back that a upgrade task has
// been completed
type Upgraded struct {
	state            State
	cert             *cert.Info
	nodeConfigSchema config.Schema
	notifierProvider NotifierProvider
}

// New creates a Upgrade task with sane defaults
func New(state State, certInfo *cert.Info, nodeConfigSchema config.Schema, options ...Option) *Upgraded {
	opts := newOptions()
	opts.state = state
	opts.certInfo = certInfo
	opts.nodeConfigSchema = nodeConfigSchema
	for _, option := range options {
		option(opts)
	}

	return &Upgraded{
		state:            opts.state,
		cert:             opts.certInfo,
		nodeConfigSchema: opts.nodeConfigSchema,
		notifierProvider: opts.notifierProvider,
	}
}

// Run sends a notification to all other nodes in the
// cluster that any possible pending database update has been applied, and any
// nodes which was waiting for this node to be upgraded should re-check if it's
// okay to move forward.
func (u *Upgraded) Run() error {
	notifierTask := u.notifierProvider.New(u.state, u.cert, u.nodeConfigSchema)
	if err := notifierTask.Run(func(client *client.Client) error {
		response, _, err := client.Query("PATCH", heartbeat.DatabaseEndpoint, nil, "")
		if err != nil {
			return errors.Wrap(err, "failed to notify node about completed upgrade")
		}

		if response.StatusCode != http.StatusOK {
			return errors.Errorf("database upgrade notification failed: %s", response.Status)
		}
		return nil
	}, notifier.NotifyAll); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

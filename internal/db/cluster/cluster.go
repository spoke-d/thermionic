package cluster

import (
	"context"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/db/schema"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/version"
	"github.com/pkg/errors"
)

// StmtInitialNode defines a transactional statement for inserting a new node
// into the db
var StmtInitialNode = `
INSERT INTO nodes 
            (id, 
             name, 
             address, 
             schema, 
             api_extensions) 
VALUES     (1, 
            'none', 
            '0.0.0.0', 
            ?, 
            ?) 
`

// DatabaseRegistrar represents a way to register and un-register drivers with
// associated name.
type DatabaseRegistrar interface {
	// Register makes a database driver available by the provided name.
	// If Register is called twice with the same name or if driver is nil,
	// it panics.
	Register(name string, driver driver.Driver)

	// Drivers returns a sorted list of the names of the registered drivers.
	Drivers() []string
}

// DatabaseOpener represents a way to open a database source
type DatabaseOpener interface {
	// Open opens a database specified by its database driver name and a
	// driver-specific data source name, usually consisting of at least a
	// database name and connection information.
	Open(driverName, dataSourceName string) (database.DB, error)
}

// DatabaseDriver represents a way to create a new database driver.
type DatabaseDriver interface {

	// Create a new driver from a given Dialer
	Create(ServerStore, ...dqlite.DriverOption) (driver.Driver, error)
}

// DatabaseIO captures all the input/output (IO) of the database in one logical
// interface.
type DatabaseIO interface {
	DatabaseDriver
	DatabaseRegistrar
	DatabaseOpener
}

// NameProvider represents a way to get names for getting names for specific
// flows.
type NameProvider interface {
	// DriverName returns a unique name for a driver.
	DriverName() string
}

// APIExtensions represents the various API Extensions there are to offer.
type APIExtensions interface {
	// Count returns the number of APIExtensions are available.
	Count() int
}

// SchemaProvider defines a factory for creating schemas
type SchemaProvider interface {

	// Schema creates a new Schema that captures the schema of a database in
	// terms of a series of ordered updates.
	Schema() Schema

	// Updates returns the schema updates that is required for the updating of
	// the database.
	Updates() []schema.Update
}

// Schema captures the schema of a database in terms of a series of ordered
// updates.
type Schema interface {

	// Fresh sets a statement that will be used to create the schema from scratch
	// when bootstraping an empty database. It should be a "flattening" of the
	// available updates, generated using the Dump() method. If not given, all
	// patches will be applied in order.
	Fresh(string)
	// File extra queries from a file. If the file is exists, all SQL queries in it
	// will be executed transactionally at the very start of Ensure(), before
	// anything else is done.
	File(string)
	// Check instructs the schema to invoke the given function whenever Ensure is
	// invoked, before applying any due update. It can be used for aborting the
	// operation.
	Check(schema.Check)
	// Hook instructs the schema to invoke the given function whenever a update is
	// about to be applied. The function gets passed the update version number and
	// the running transaction, and if it returns an error it will cause the schema
	// transaction to be rolled back. Any previously installed hook will be
	// replaced.
	Hook(schema.Hook)

	// Ensure makes sure that the actual schema in the given database matches the
	// one defined by our updates.
	//
	// All updates are applied transactionally. In case any error occurs the
	// transaction will be rolled back and the database will remain unchanged.
	Ensure(database.DB) (int, error)
}

// ServerInfo is the Go equivalent of dqlite_server_info.
type ServerInfo struct {
	ID      uint64
	Address string
}

// ServerStore is used by a dqlite client to get an initial list of candidate
// dqlite servers that it can dial in order to find a leader server to connect
// to.
//
// Once connected, the client periodically updates the server addresses in the
// store by querying the leader about changes in the cluster (such as servers
// being added or removed).
type ServerStore interface {
	// Get return the list of known servers.
	Get(context.Context) ([]ServerInfo, error)

	// Set updates the list of known cluster servers.
	Set(context.Context, []ServerInfo) error
}

// Cluster represents a cluster of database nodes, in the sense that you can
// open and query a cluster.
type Cluster struct {
	database       database.DB
	databaseIO     DatabaseIO
	nameProvider   NameProvider
	schemaProvider SchemaProvider
	apiExtensions  APIExtensions
	fileSystem     fsys.FileSystem
	sleeper        clock.Sleeper
	once           sync.Once
}

// New creates a cluster ensuring that sane defaults are employed.
func New(apiExtensions APIExtensions, schemaProvider SchemaProvider, options ...Option) *Cluster {
	opts := newOptions()
	opts.apiExtensions = apiExtensions
	opts.schemaProvider = schemaProvider
	for _, option := range options {
		option(opts)
	}

	return &Cluster{
		database:       opts.database,
		databaseIO:     opts.databaseIO,
		nameProvider:   opts.nameProvider,
		schemaProvider: opts.schemaProvider,
		apiExtensions:  opts.apiExtensions,
		fileSystem:     opts.fileSystem,
		sleeper:        opts.sleeper,
	}
}

// Open the cluster database object.
//
// The name argument is the name of the cluster database. It defaults to
// 'db.bin', but can be overwritten for testing.
//
// The dialer argument is a function that returns a gRPC dialer that can be
// used to connect to a database node using the gRPC SQL package.
func (c *Cluster) Open(store ServerStore, options ...dqlite.DriverOption) error {
	driver, err := c.databaseIO.Create(store, options...)
	if err != nil {
		return errors.WithStack(err)
	}
	driverName := c.nameProvider.DriverName()

	c.register(driverName, driver)

	// Create the cluster database. This won't immediately establish any gRPC
	// connection, that will happen only when a db transaction is started
	// (see the database/sql connection pooling code for more details).
	db, err := c.databaseIO.Open(driverName, "db.bin?_foreign_keys=1")

	c.database = db

	return errors.Wrap(err, "cannot open cluster database")
}

// EnsureSchema applies all relevant schema updates to the cluster database.
//
// Before actually doing anything, this function will make sure that all nodes
// in the cluster have a schema version and a number of API extensions that
// match our one. If it's not the case, we either return an error (if some
// nodes have version greater than us and we need to be upgraded), or return
// false and no error (if some nodes have a lower version, and we need to wait
// till they get upgraded and restarted).
func (c *Cluster) EnsureSchema(address string, dir string) (bool, error) {
	ctx := &hookContext{}

	numAPIExtensions := c.apiExtensions.Count()
	numSchemaUpdates := len(c.schemaProvider.Updates())

	schema := c.schemaProvider.Schema()
	schema.File(filepath.Join(dir, "patch.global.sql"))
	schema.Check(func(version int, tx database.Tx) error {
		err := check(ctx, address, version, numSchemaUpdates, numAPIExtensions, tx)
		return errors.WithStack(err)
	})
	schema.Hook(func(version int, tx database.Tx) error {
		err := hook(ctx, c.fileSystem, dir, version, tx)
		return errors.WithStack(err)
	})

	var initial int
	err := query.Retry(c.sleeper, func() error {
		var err error
		initial, err = schema.Ensure(c.database)
		return err
	})
	if ctx.someNodesAreBehind {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// When creating a database from scratch, insert an entry for node
	// 1. This is needed for referential integrity with other tables. Also,
	// create a default profile.
	if initial == 0 {
		tx, err := c.database.Begin()
		if err != nil {
			return false, errors.WithStack(err)
		}
		numAPIExtensions := c.apiExtensions.Count()
		if _, err := tx.Exec(StmtInitialNode, numSchemaUpdates, numAPIExtensions); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return false, errors.Wrap(err, rollbackErr.Error())
			}
			return false, errors.WithStack(err)
		}

		if err := tx.Commit(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return true, nil
}

// DB return the current database source.
func (c *Cluster) DB() database.DB {
	return c.database
}

// SchemaVersion returns the underlying schema version for the cluster
func (c *Cluster) SchemaVersion() int {
	return len(c.schemaProvider.Updates())
}

func (c *Cluster) register(driverName string, driver driver.Driver) {
	c.once.Do(func() {
		drivers := c.databaseIO.Drivers()
		for _, v := range drivers {
			if v == driverName {
				return
			}
		}
		c.databaseIO.Register(driverName, driver)
	})
}

type hookContext struct {
	backupDone         bool
	someNodesAreBehind bool
}

func hook(ctx *hookContext, fsys fsys.FileSystem, dir string, version int, tx database.Tx) error {
	// Check if this is a fresh instance.
	isUpdate, err := schema.SchemaTableExists(tx)
	if err != nil {
		return errors.Wrap(err, "failed to check if schema table exists")
	}
	if !isUpdate {
		return nil
	}

	//Check if we're clustered
	clustered := true
	n, err := selectUnclusteredNodesCount(tx)
	if err != nil {
		return errors.Wrap(err, "failed to fetch unclustered nodes count")
	}
	if n > 1 {
		// This should never happen, since we only add nodes
		// with valid addresses, but check it for sanity.
		return errors.Errorf("found more than one unclustered nodes")
	} else if n == 1 {
		clustered = false
	}

	// If we're not clustered, backup the local cluster database directory
	// before performing any schema change. This makes sense only in the
	// non-clustered case, because otherwise the directory would be
	// re-populated by replication.
	if !clustered && !ctx.backupDone {
		err := fsys.CopyDir(
			filepath.Join(dir, "global"),
			filepath.Join(dir, "global.bak"),
		)
		if err != nil {
			return errors.Wrap(err, "failed to backup global database")
		}
		ctx.backupDone = true
	}
	return nil
}

func check(ctx *hookContext, address string, current, updates, apiExtensions int, tx database.Tx) error {
	// If we're bootstrapping a fresh schema, skip any check, since
	// it's safe to assume we are the only node.
	if current == 0 {
		return nil
	}

	// Check if we're clustered
	n, err := selectUnclusteredNodesCount(tx)
	if err != nil {
		return errors.Wrap(err, "failed to fetch unclustered nodes count")
	}
	if n > 1 {
		// This should never happen, since we only add nodes with valid
		// addresses, but check it for sanity.
		return errors.Errorf("found more than one unclustered nodes")
	} else if n == 1 {
		address = "0.0.0.0"
	}

	// Update the schema and api_extension columns of oursleves.
	if err := updateNodeVersion(tx, address, updates, apiExtensions); err != nil {
		return errors.Wrap(err, "failed to update node version info")
	}

	err = checkClusterIsUpgradable(tx, [2]int{
		updates,
		apiExtensions,
	})
	if err == ErrSomeNodesAreBehind {
		ctx.someNodesAreBehind = true
		return schema.ErrGracefulAbort
	}
	return errors.WithStack(err)
}

func checkClusterIsUpgradable(tx database.Tx, target [2]int) error {
	// Get the current versions in the nodes table.
	versions, err := selectNodesVersions(tx)
	if err != nil {
		return errors.Wrap(err, "failed to fetch current nodes versions")
	}

	for _, v := range versions {
		n, err := version.CompareVersions(target, v)
		if err != nil {
			return err
		}
		switch n {
		case 0:
			// Versions are equal, there's hope for the
			// update. Let's check the next node.
			continue
		case 1:
			// Our version is bigger, we should stop here
			// and wait for other nodes to be upgraded and
			// restarted.
			return ErrSomeNodesAreBehind
		case 2:
			// Another node has a version greater than ours
			// and presumeably is waiting for other nodes
			// to upgrade. Let's error out and shutdown
			// since we need a greater version.
			return errors.Errorf("this node's version is behind, please upgrade")
		default:
			// Sanity.
			panic("unexpected return value from compareVersions")
		}
	}
	return nil
}

// Monotonic serial number for registering new instances of dqlite.Driver
// using the database/sql stdlib package. This is needed since there's no way
// to unregister drivers, and in unit tests more than one driver gets
// registered.
var dqliteDriverSerial uint64

// DQliteNameProvider creates a new name provider for the cluster
type DQliteNameProvider struct{}

// DriverName generates a new name for the dqlite driver registration. We need
// it to be unique for testing, see below.
func (p *DQliteNameProvider) DriverName() string {
	defer atomic.AddUint64(&dqliteDriverSerial, 1)
	return fmt.Sprintf("dqlite-%d", dqliteDriverSerial)
}

// BasicAPIExtensions represents the various API Extensions there are to offer.
type BasicAPIExtensions struct {
	count int
}

// NewBasicAPIExtensions creates simple APIExtensions provider
func NewBasicAPIExtensions(count int) *BasicAPIExtensions {
	return &BasicAPIExtensions{
		count: count,
	}
}

// Count returns the number of APIExtensions are available.
func (e *BasicAPIExtensions) Count() int {
	return e.count
}

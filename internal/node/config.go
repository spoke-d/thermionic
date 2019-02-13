package node

import (
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/pkg/errors"
)

// Tx models a single interaction with a node-local database.
type Tx interface {

	// Config fetches all node-level config keys.
	Config() (map[string]string, error)

	// UpdateConfig updates the given node-level configuration keys in the
	// config table. Config keys set to empty values will be deleted.
	UpdateConfig(map[string]string) error

	// RaftNodes returns information about all nodes that are members of the
	// dqlite Raft cluster (possibly including the local node). If this
	// instance is not running in clustered mode, an empty list is returned.
	RaftNodes() ([]db.RaftNode, error)
}

// Option to be passed to New to customize the resulting
// instance.
type Option func(*options)

type options struct {
	nodeTx    Tx
	configMap config.Map
}

// NodeTx sets the nodeTx on the options
func NodeTx(nodeTx Tx) Option {
	return func(options *options) {
		options.nodeTx = nodeTx
	}
}

// Map sets the configMap on the options
func Map(configMap config.Map) Option {
	return func(options *options) {
		options.configMap = configMap
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{}
}

// ConfigSchema defines available server configuration keys.
var ConfigSchema = config.Schema{
	// Network address for this server
	"core.https_address": {},

	// Network address for the debug server
	"core.debug_address": {},
}

// Config holds node-local configuration values for a certain instance
type Config struct {
	nodeTx    Tx         // DB transaction the values in this config are bound to
	configMap config.Map // Low-level map holding the config values.
}

// NewConfig creates a Config with sane defaults.
func NewConfig(options ...Option) *Config {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Config{
		nodeTx:    opts.nodeTx,
		configMap: opts.configMap,
	}
}

// ConfigLoad loads a new Config object with the current node-local configuration
// values fetched from the database. An optional list of config value triggers
// can be passed, each config key must have at most one trigger.
func ConfigLoad(tx Tx, schema config.Schema) (*Config, error) {
	// Load current raw values from the database, any error is fatal.
	values, err := tx.Config()
	if err != nil {
		return nil, errors.Errorf("cannot fetch node config from database: %v", err)
	}

	m, err := config.New(schema, values)
	if err != nil {
		return nil, errors.Errorf("failed to load node config: %v", err)
	}

	return NewConfig(
		NodeTx(tx),
		Map(m),
	), nil
}

// HTTPSAddress returns the address and port this node should expose its
// API to, if any.
func (c *Config) HTTPSAddress() (string, error) {
	return c.configMap.GetString("core.https_address")
}

// DebugAddress returns the address and port to setup the pprof listener on
func (c *Config) DebugAddress() (string, error) {
	return c.configMap.GetString("core.debug_address")
}

// Dump current configuration keys and their values. Keys with values matching
// their defaults are omitted.
func (c *Config) Dump() (map[string]interface{}, error) {
	return c.configMap.Dump()
}

// Replace the current configuration with the given values.
func (c *Config) Replace(values map[string]interface{}) (map[string]string, error) {
	return c.update(values)
}

// Patch changes only the configuration keys in the given map.
func (c *Config) Patch(patch map[string]interface{}) (map[string]string, error) {
	values, err := c.Dump() // Use current values as defaults
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for name, value := range patch {
		values[name] = value
	}
	return c.update(values)
}

func (c *Config) update(values map[string]interface{}) (map[string]string, error) {
	changed, err := c.configMap.Change(values)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	err = c.nodeTx.UpdateConfig(changed)
	if err != nil {
		return nil, errors.Wrap(err, "cannot persist local configuration changes")
	}

	return changed, nil
}

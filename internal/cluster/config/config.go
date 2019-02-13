package config

import (
	"strconv"
	"time"

	"github.com/spoke-d/thermionic/internal/cluster/heartbeat"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/pkg/errors"
)

// Tx models a single interaction with a cluster database.
type Tx interface {

	// Config fetches all node-level config keys.
	Config() (map[string]string, error)

	// UpdateConfig updates the given node-level configuration keys in the
	// config table. Config keys set to empty values will be deleted.
	UpdateConfig(map[string]string) error
}

// ReadOnlyConfig only allows the reading of values from the map
type ReadOnlyConfig struct {
	configMap config.Map // Low-level map holding the config values.
}

// DiscoveryAddress returns the API address for the discovery service
func (c *ReadOnlyConfig) DiscoveryAddress() (string, error) {
	return c.configMap.GetString("discovery.https_address")
}

// DiscoveryOfflineThreshold returns the configured heartbeat threshold, i.e.
// the number of seconds before after which an unresponsive service node is
// considered offline..
func (c *ReadOnlyConfig) DiscoveryOfflineThreshold() (time.Duration, error) {
	n, err := c.configMap.GetInt64("discovery.offline_threshold")
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return time.Duration(n) * time.Second, nil
}

// HTTPSAllowedHeaders returns the relevant CORS setting.
func (c *ReadOnlyConfig) HTTPSAllowedHeaders() (string, error) {
	return c.configMap.GetString("core.https_allowed_headers")
}

// HTTPSAllowedMethods returns the relevant CORS setting.
func (c *ReadOnlyConfig) HTTPSAllowedMethods() (string, error) {
	return c.configMap.GetString("core.https_allowed_methods")
}

// HTTPSAllowedOrigin returns the relevant CORS setting.
func (c *ReadOnlyConfig) HTTPSAllowedOrigin() (string, error) {
	return c.configMap.GetString("core.https_allowed_origin")
}

// HTTPSAllowedCredentials returns the relevant CORS setting.
func (c *ReadOnlyConfig) HTTPSAllowedCredentials() (bool, error) {
	return c.configMap.GetBool("core.https_allowed_credentials")
}

// ProxyHTTPS returns the configured HTTPS proxy, if any.
func (c *ReadOnlyConfig) ProxyHTTPS() (string, error) {
	return c.configMap.GetString("core.proxy_https")
}

// ProxyHTTP returns the configured HTTP proxy, if any.
func (c *ReadOnlyConfig) ProxyHTTP() (string, error) {
	return c.configMap.GetString("core.proxy_http")
}

// OfflineThreshold returns the configured heartbeat threshold, i.e. the
// number of seconds before after which an unresponsive node is considered
// offline..
func (c *ReadOnlyConfig) OfflineThreshold() (time.Duration, error) {
	n, err := c.configMap.GetInt64("cluster.offline_threshold")
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return time.Duration(n) * time.Second, nil
}

// NewReadOnlyConfig creates a read only configuration with values
func NewReadOnlyConfig(values map[string]string, schema config.Schema) (*ReadOnlyConfig, error) {
	configMap, err := config.New(schema, values)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load config")
	}

	return &ReadOnlyConfig{
		configMap: configMap,
	}, nil
}

// Dump current configuration keys and their values. Keys with values matching
// their defaults are omitted.
func (c *Config) Dump() (map[string]interface{}, error) {
	return c.configMap.Dump()
}

// Config holds cluster-wide configuration values.
type Config struct {
	*ReadOnlyConfig
	clusterTx Tx // DB transaction the values in this config are bound to.
}

// NewConfig creates a Config with sane defaults.
func NewConfig(configMap config.Map, options ...Option) *Config {
	opts := newOptions()
	opts.configMap = configMap
	for _, option := range options {
		option(opts)
	}

	return &Config{
		ReadOnlyConfig: &ReadOnlyConfig{
			configMap: opts.configMap,
		},
		clusterTx: opts.clusterTx,
	}
}

// Load a new Config object with the current cluster configuration
// values fetched from the database.
func Load(tx Tx, schema config.Schema) (*Config, error) {
	// Load current raw values from the database, any error is fatal.
	values, err := tx.Config()
	if err != nil {
		return nil, errors.Wrap(err, "cannot fetch node config from database")
	}

	configMap, err := config.New(schema, values)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load node config")
	}

	return &Config{
		ReadOnlyConfig: &ReadOnlyConfig{
			configMap: configMap,
		},
		clusterTx: tx,
	}, nil
}

// ReadOnly returns the underlying readonly map
func (c *Config) ReadOnly() *ReadOnlyConfig {
	return &ReadOnlyConfig{
		configMap: c.ReadOnlyConfig.configMap.Clone(),
	}
}

// Replace the current configuration with the given values.
//
// Return what has actually changed.
func (c *Config) Replace(values map[string]interface{}) (map[string]string, error) {
	return c.update(values)
}

// Patch changes only the configuration keys in the given map.
//
// Return what has actually changed.
func (c *Config) Patch(patch map[string]interface{}) (map[string]string, error) {
	values, err := c.Dump()
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
		return nil, err
	}

	err = c.clusterTx.UpdateConfig(changed)
	if err != nil {
		return nil, errors.Wrap(err, "cannot persist configuration changes: %v")
	}

	return changed, nil
}

// Schema defines available server configuration keys.
var Schema = config.Schema{
	"cluster.offline_threshold":      {Type: config.Int64, Default: clusterOfflineThresholdDefault(), Validator: offlineThresholdValidator},
	"core.proxy_http":                {},
	"core.proxy_https":               {},
	"core.https_allowed_headers":     {},
	"core.https_allowed_methods":     {},
	"core.https_allowed_origin":      {},
	"core.https_allowed_credentials": {Type: config.Bool},
	"discovery.https_address":        {},
	"discovery.offline_threshold":    {Type: config.Int64, Default: discoveryOfflineThresholdDefault(), Validator: offlineThresholdValidator},
}

func clusterOfflineThresholdDefault() string {
	return strconv.Itoa(db.ClusterDefaultOfflineThreshold)
}

func discoveryOfflineThresholdDefault() string {
	return strconv.Itoa(db.DiscoveryDefaultOfflineThreshold)
}

func offlineThresholdValidator(value string) error {
	// Ensure that the given value is greater than the heartbeat interval,
	// which is the lower bound granularity of the offline check.
	threshold, err := strconv.Atoi(value)
	if err != nil {
		return errors.Errorf("offline threshold is not a number")
	}
	if threshold <= heartbeat.Interval {
		return errors.Errorf("value must be greater than '%d'", heartbeat.Interval)
	}
	return nil
}

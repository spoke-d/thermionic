package cluster

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/fsys"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/pkg/api"
)

// API defines a query API
type API struct {
	api.DefaultService

	name string

	logger     log.Logger
	fileSystem fsys.FileSystem
	clock      clock.Clock
}

// NewAPI creates a API with sane defaults
func NewAPI(name string, options ...Option) *API {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &API{
		name:       name,
		logger:     opts.logger,
		fileSystem: opts.fileSystem,
		clock:      opts.clock,
	}
}

// Name returns the API name
func (a *API) Name() string {
	return a.name
}

// Get defines a service for calling "GET" method and returns a response.
// Return information about the cluster.
func (a *API) Get(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var name string
	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		name, err = tx.NodeName()
		return errors.WithStack(err)
	}); err != nil {
		return api.SmartError(err)
	}

	// If the name is set to the hard-coded default node name, then
	// clustering is not enabled.
	if name == "none" {
		name = ""
	}

	cluster := Cluster{
		ServerName: name,
		Enabled:    name != "",
	}

	return api.SyncResponseETag(true, cluster, cluster)
}

// Put defines a service for calling "PUT" method and returns a response.
// Depending on the parameters passed and on local state this endpoint will
// either:
//
// - bootstrap a new cluster (if this node is not clustered yet)
// - request to join an existing cluster
// - disable clustering on a node
func (a *API) Put(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var info ClusterJoin
	if err := json.NewDecoder(req.Body).Decode(&info); err != nil {
		return api.BadRequest(err)
	}

	if info.ServerName == "" && info.Enabled {
		return api.BadRequest(errors.Errorf("server name is required when enabling clustering"))
	}
	if info.ServerName != "" && !info.Enabled {
		return api.BadRequest(errors.Errorf("server name must be empty when disabling clustering"))
	}

	switch {
	case !info.Enabled:
		return clusterDisable(d, a.logger, a.fileSystem)
	case info.ClusterAddress == "":
		return clusterBootstrap(d, info, a.logger, a.fileSystem, a.clock)
	default:
		return clusterJoin(d, info, a.logger, a.fileSystem, a.clock)
	}
}

// Cluster represents high-level information about the cluster.
type Cluster struct {
	ServerName string `json:"server_name" yaml:"server_name"`
	Enabled    bool   `json:"enabled" yaml:"enabled"`
}

// ClusterJoin represents the fields required to bootstrap or join a cluster.
type ClusterJoin struct {
	Cluster            `yaml:",inline"`
	ClusterAddress     string `json:"cluster_address" yaml:"cluster_address"`
	ClusterCertificate string `json:"cluster_certificate" yaml:"cluster_certificate"`
	ClusterKey         string `json:"cluster_key" yaml:"cluster_key"`
	ServerAddress      string `json:"server_address" yaml:"server_address"`
}

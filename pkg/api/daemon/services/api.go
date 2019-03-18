package services

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/pkg/api"
)

// API defines a query API
type API struct {
	api.DefaultService
	name       string
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

// Post defines a service for calling "POST" method and returns a response.
func (a *API) Post(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var services []ServiceNode
	if err := json.NewDecoder(req.Body).Decode(&services); err != nil {
		return api.BadRequest(err)
	}

	for _, node := range services {
		if node.ServerName == "" {
			return api.BadRequest(errors.Errorf("server name is required"))
		}
		if node.ServerAddress == "" {
			return api.BadRequest(errors.Errorf("server address is required"))
		}
		if node.DaemonAddress == "" {
			return api.BadRequest(errors.Errorf("daemon address is required"))
		}
	}

	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		for _, node := range services {
			if _, err := tx.ServiceAdd(
				node.ServerName,
				node.ServerAddress,
				node.DaemonAddress,
				node.DaemonNonce,
			); err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	}); err != nil {
		return api.SmartError(err)
	}
	return api.EmptySyncResponse()
}

// ServiceResult represents a node part of the raft cluster
type ServiceResult struct {
	ServiceNode `json:",inline" yaml:",inline"`
	Status      string `json:"status" yaml:"status"`
	Message     string `json:"message" yaml:"message"`
}

// ServiceNode represents a request to update the node for the raft
// cluster
type ServiceNode struct {
	ServerName    string `json:"server_name" yaml:"server_name"`
	ServerAddress string `json:"server_address" yaml:"server_address"`
	DaemonAddress string `json:"daemon_address" yaml:"daemon_address"`
	DaemonNonce   string `json:"daemon_nonce" yaml:"daemon_nonce"`
}

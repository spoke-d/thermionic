package services

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster/services"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

// MembersAPI defines a query MembersAPI
type MembersAPI struct {
	api.DefaultService
	name       string
	logger     log.Logger
	fileSystem fsys.FileSystem
	clock      clock.Clock
}

// NewMembersAPI creates a MembersAPI with sane defaults
func NewMembersAPI(name string, options ...Option) *MembersAPI {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &MembersAPI{
		name:       name,
		logger:     opts.logger,
		fileSystem: opts.fileSystem,
		clock:      opts.clock,
	}
}

// Name returns the MembersAPI name
func (a *MembersAPI) Name() string {
	return a.name
}

// Get defines a service for calling "POST" method and returns a response.
func (a *MembersAPI) Get(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	listTask := services.NewList(
		makeServicesStateShim(d.State()),
	)
	nodes, err := listTask.Run()
	if err != nil {
		return api.SmartError(err)
	}

	result := make([]ServiceResult, len(nodes))
	for k, v := range nodes {
		result[k] = ServiceResult{
			ServiceNode: ServiceNode{
				ServerName:    v.ServerName,
				ServerAddress: v.ServerAddress,
				DaemonAddress: v.DaemonAddress,
				DaemonNonce:   v.DaemonNonce,
			},
			Status:  v.Status,
			Message: v.Message,
		}
	}

	return api.SyncResponse(true, result)
}

// Post defines a service for calling "POST" method and returns a response.
func (a *MembersAPI) Post(ctx context.Context, req *http.Request) api.Response {
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
		if node.DaemonNonce == "" {
			return api.BadRequest(errors.Errorf("daemon nonce is required"))
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

// Delete defines a service for calling "DELETE" method and returns a response.
func (a *MembersAPI) Delete(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var services []ServiceNode
	if err := json.NewDecoder(req.Body).Decode(&services); err != nil {
		return api.BadRequest(err)
	}

	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		for _, node := range services {
			info, err := tx.ServiceByName(node.ServerName)
			if err != nil {
				return errors.WithStack(err)
			}
			return tx.ServiceRemove(info.ID)
		}
		return nil
	}); err != nil {
		return api.SmartError(err)
	}
	return api.EmptySyncResponse()
}

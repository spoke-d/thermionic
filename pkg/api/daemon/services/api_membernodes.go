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
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// MemberNodesAPI defines a query MemberNodesAPI
type MemberNodesAPI struct {
	api.DefaultService
	name       string
	logger     log.Logger
	fileSystem fsys.FileSystem
	clock      clock.Clock
}

// NewMemberNodesAPI creates a MemberNodesAPI with sane defaults
func NewMemberNodesAPI(name string, options ...Option) *MemberNodesAPI {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &MemberNodesAPI{
		name:       name,
		logger:     opts.logger,
		fileSystem: opts.fileSystem,
		clock:      opts.clock,
	}
}

// Name returns the MemberNodesAPI name
func (a *MemberNodesAPI) Name() string {
	return a.name
}

// Get defines a service for calling "POST" method and returns a response.
// Return information about the cluster.
func (a *MemberNodesAPI) Get(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	name, ok := mux.Vars(req)["name"]
	if !ok {
		return api.BadRequest(errors.Errorf("expected name"))
	}

	listTask := services.NewList(
		makeServicesStateShim(d.State()),
	)
	nodes, err := listTask.Run()
	if err != nil {
		return api.SmartError(err)
	}

	for _, v := range nodes {
		if v.ServerName == name {
			return api.SyncResponse(true, ServiceResult{
				ServiceNode: ServiceNode{
					ServerName:    v.ServerName,
					ServerAddress: v.ServerAddress,
				},
				Status:  v.Status,
				Message: v.Message,
			})
		}
	}

	return api.NotFound(errors.Errorf("node not found for %q", name))
}

// Post defines a service for calling "POST" method and returns a response.
func (a *MemberNodesAPI) Post(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	name, ok := mux.Vars(req)["name"]
	if !ok {
		return api.BadRequest(errors.Errorf("expected name"))
	}

	var data ServiceNode
	if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
		return api.BadRequest(err)
	}

	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		info, err := tx.ServiceByName(name)
		if err != nil {
			return errors.WithStack(err)
		}
		return tx.ServiceUpdate(
			info.ID,
			data.ServerName,
			data.ServerAddress,
			data.DaemonAddress,
			data.DaemonNonce,
		)
	}); err != nil {
		return api.SmartError(err)
	}
	return api.EmptySyncResponse()
}

// Delete defines a service for calling "DELETE" method and returns a response.
func (a *MemberNodesAPI) Delete(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	name, ok := mux.Vars(req)["name"]
	if !ok {
		return api.BadRequest(errors.Errorf("expected name"))
	}
	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		info, err := tx.ServiceByName(name)
		if err != nil {
			return errors.WithStack(err)
		}
		return tx.ServiceRemove(info.ID)
	}); err != nil {
		return api.SmartError(err)
	}

	return api.EmptySyncResponse()
}

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/pkg/api"
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

	listTask := membership.NewList(
		makeMembershipStateShim(d.State()),
	)
	nodes, err := listTask.Run()
	if err != nil {
		return api.SmartError(err)
	}

	for _, v := range nodes {
		if v.ServerName == name {
			return api.SyncResponse(true, ClusterRaftNode{
				ServerName: v.ServerName,
				URL:        v.URL,
				Database:   v.Database,
				Status:     v.Status,
				Message:    v.Message,
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

	var info ClusterRenameRequest
	if err := json.NewDecoder(req.Body).Decode(&info); err != nil {
		return api.BadRequest(err)
	}

	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		return tx.NodeRename(name, info.ServerName)
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

	force, err := strconv.Atoi(req.FormValue("force"))
	if err != nil {
		force = 0
	}

	leaveTask := membership.NewLeave(
		makeMembershipStateShim(d.State()),
		makeMembershipGatewayShim(d.Gateway()),
	)
	address, err := leaveTask.Run(name, force == 1)
	if err != nil {
		return api.SmartError(err)
	}

	if force != 1 {
		// Handle the graceful shutdown of a node, before purging it.
	}

	purgeTask := membership.NewPurge(
		makeMembershipStateShim(d.State()),
	)
	if err := purgeTask.Run(name); err != nil {
		return api.SmartError(errors.Wrap(err, "failed to remove node from database"))
	}

	// Try and rebalance the leader
	if err := tryClusterRebalance(d, a.logger); err != nil {
		level.Error(a.logger).Log("msg", "failed to rebalance cluster", "err", err)
	}

	if force != 1 {
		certInfo := d.Endpoints().NetworkCert()
		client, err := getClient(address, certInfo, a.logger)
		if err != nil {
			return api.SmartError(err)
		}
		data := ClusterJoin{
			Cluster: Cluster{
				Enabled: false,
			},
		}
		if _, _, err := client.Query("PUT", fmt.Sprintf("/%s/cluster", d.Version()), data, ""); err != nil {
			return api.SmartError(errors.Wrap(err, "failed to cleanup the node"))
		}
	}

	return api.EmptySyncResponse()
}

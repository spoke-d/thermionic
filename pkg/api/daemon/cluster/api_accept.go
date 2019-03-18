package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/pkg/api"
)

// AcceptAPI defines a query AcceptAPI
type AcceptAPI struct {
	api.DefaultService
	name       string
	logger     log.Logger
	fileSystem fsys.FileSystem
	clock      clock.Clock
}

// NewAcceptAPI creates a AcceptAPI with sane defaults
func NewAcceptAPI(name string, options ...Option) *AcceptAPI {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &AcceptAPI{
		name:       name,
		logger:     opts.logger,
		fileSystem: opts.fileSystem,
		clock:      opts.clock,
	}
}

// Name returns the AcceptAPI name
func (a *AcceptAPI) Name() string {
	return a.name
}

// Post defines a service for calling "POST" method and returns a response.
// Return information about the cluster.
func (a *AcceptAPI) Post(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var info ClusterAcceptRequest
	if err := json.NewDecoder(req.Body).Decode(&info); err != nil {
		return api.BadRequest(err)
	}

	// Sanity checks
	if info.Name == "" {
		return api.BadRequest(errors.Errorf("no name provided"))
	}

	// Redirect all requests to the leader, which is the one with
	// knowning what nodes are part of the raft cluster.
	address, err := node.HTTPSAddress(d.Node(), d.NodeConfigSchema())
	if err != nil {
		return api.SmartError(err)
	}
	leader, err := d.Gateway().LeaderAddress()
	if err != nil {
		return api.InternalError(err)
	}
	if address != leader {
		level.Debug(a.logger).Log("msg", "redirect node accept request", "leader", leader)
		url := &url.URL{
			Scheme: "https",
			Path:   "/internal/cluster/accept",
			Host:   leader,
		}
		return api.SyncResponseRedirect(url.String())
	}

	acceptTask := membership.NewAccept(
		makeMembershipStateShim(d.State()),
		makeMembershipGatewayShim(d.Gateway()),
	)
	nodes, err := acceptTask.Run(info.Name, info.Address, info.Schema, info.API)
	if err != nil {
		return api.BadRequest(err)
	}
	accepted := ClusterAcceptResponse{
		RaftNodes:  make([]RaftNode, len(nodes)),
		PrivateKey: d.Endpoints().NetworkPrivateKey(),
	}
	for i, node := range nodes {
		accepted.RaftNodes[i].ID = node.ID
		accepted.RaftNodes[i].Address = node.Address
	}
	return api.SyncResponse(true, accepted)
}

package cluster

import (
	"context"
	libjson "encoding/json"
	"errors"
	"net/http"

	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/go-kit/kit/log"
)

// PromoteAPI defines a query PromoteAPI
type PromoteAPI struct {
	api.DefaultService
	name       string
	logger     log.Logger
	fileSystem fsys.FileSystem
	clock      clock.Clock
}

// NewPromoteAPI creates a PromoteAPI with sane defaults
func NewPromoteAPI(name string, options ...Option) *PromoteAPI {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &PromoteAPI{
		name:       name,
		logger:     opts.logger,
		fileSystem: opts.fileSystem,
		clock:      opts.clock,
	}
}

// Name returns the PromoteAPI name
func (a *PromoteAPI) Name() string {
	return a.name
}

// Post defines a service for calling "POST" method and returns a response.
// Return information about the cluster.
func (a *PromoteAPI) Post(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var info ClusterPromoteRequest
	if err := libjson.NewDecoder(req.Body).Decode(&info); err != nil {
		return api.BadRequest(err)
	}

	if len(info.RaftNodes) == 0 {
		return api.BadRequest(errors.New("no raft nodes provided"))
	}

	nodes := make([]db.RaftNode, len(info.RaftNodes))
	for i, node := range info.RaftNodes {
		nodes[i].ID = node.ID
		nodes[i].Address = node.Address
	}

	promoteTask := membership.NewPromote(
		makeMembershipStateShim(d.State()),
		makeMembershipGatewayShim(d.Gateway()),
	)
	if err := promoteTask.Run(d.Endpoints().NetworkCert(), nodes); err != nil {
		return api.SmartError(err)
	}

	return api.EmptySyncResponse()
}

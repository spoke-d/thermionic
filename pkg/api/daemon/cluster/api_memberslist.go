package cluster

import (
	"context"
	"net/http"

	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/go-kit/kit/log"
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
// Return information about the cluster.
func (a *MembersAPI) Get(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	listTask := membership.NewList(
		makeMembershipStateShim(d.State()),
	)
	nodes, err := listTask.Run()
	if err != nil {
		return api.SmartError(err)
	}

	result := make([]ClusterRaftNode, len(nodes))
	for k, v := range nodes {
		result[k] = ClusterRaftNode{
			ServerName: v.ServerName,
			URL:        v.URL,
			Database:   v.Database,
			Status:     v.Status,
			Message:    v.Message,
		}
	}

	return api.SyncResponse(true, result)
}

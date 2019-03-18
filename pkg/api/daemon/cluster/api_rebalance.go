package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/pkg/api"
)

// RebalanceAPI defines a query RebalanceAPI
type RebalanceAPI struct {
	api.DefaultService
	name       string
	logger     log.Logger
	fileSystem fsys.FileSystem
	clock      clock.Clock
}

// NewRebalanceAPI creates a RebalanceAPI with sane defaults
func NewRebalanceAPI(name string, options ...Option) *RebalanceAPI {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &RebalanceAPI{
		name:       name,
		logger:     opts.logger,
		fileSystem: opts.fileSystem,
		clock:      opts.clock,
	}
}

// Name returns the RebalanceAPI name
func (a *RebalanceAPI) Name() string {
	return a.name
}

// Post defines a service for calling "POST" method and returns a response.
// Return information about the cluster.
func (a *RebalanceAPI) Post(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	// Redirect all requests to the leader, which is the one with with
	// up-to-date knowledge of what nodes are part of the raft cluster.
	localAddress, err := node.HTTPSAddress(d.Node(), d.NodeConfigSchema())
	if err != nil {
		return api.SmartError(err)
	}
	leader, err := d.Gateway().LeaderAddress()
	if err != nil {
		return api.InternalError(err)
	}
	if localAddress != leader {
		level.Debug(a.logger).Log("msg", "redirect cluster rebalance request", "leader", leader)
		url := &url.URL{
			Scheme: "https",
			Path:   "/internal/cluster/rebalance",
			Host:   leader,
		}
		return api.SyncResponseRedirect(url.String())
	}

	level.Debug(a.logger).Log("msg", "rebalance cluster")

	rebalanceTask := membership.NewRebalance(
		makeMembershipStateShim(d.State()),
		makeMembershipGatewayShim(d.Gateway()),
		d.ClusterConfigSchema(),
	)
	address, nodes, err := rebalanceTask.Run()
	if err != nil {
		return api.SmartError(err)
	}
	if address == "" {
		return api.SyncResponse(true, nil)
	}

	var data ClusterPromoteRequest
	for _, node := range nodes {
		data.RaftNodes = append(data.RaftNodes, RaftNode{
			ID:      node.ID,
			Address: node.Address,
		})
	}

	certInfo := d.Endpoints().NetworkCert()
	client, err := getClient(address, certInfo, a.logger)
	if err != nil {
		return api.SmartError(err)
	}

	response, _, err := client.Query("POST", "/internal/cluster/promote", data, "")
	if err != nil {
		return api.SmartError(err)
	} else if response.StatusCode != 200 {
		return api.SmartError(errors.New("invalid status code"))
	}

	return api.EmptySyncResponse()
}

package cluster

import (
	"bytes"
	libjson "encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/internal/net"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/operations"
	"github.com/spoke-d/thermionic/pkg/api"
	ops "github.com/spoke-d/thermionic/pkg/api/daemon/operations"
	"github.com/spoke-d/thermionic/pkg/client"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// disable clustering on a node
func clusterDisable(d api.Daemon, logger log.Logger, fileSystem fsys.FileSystem) api.Response {
	collapseTask := membership.NewCollapse(
		makeMembershipStateShim(d.State()),
		makeMembershipGatewayShim(d.Gateway()),
		makeMembershipEndpointsShim(d.Endpoints()),
		d.NodeConfigSchema(),
		membership.WithLoggerForCollapse(logger),
		membership.WithFileSystemForCollapse(fileSystem),
	)
	cluster, err := collapseTask.Run(d.APIExtensions())
	if err != nil {
		return api.SmartError(err)
	}

	d.UnsafeSetCluster(cluster)

	return api.EmptySyncResponse()
}

func clusterBootstrap(
	d api.Daemon,
	info ClusterJoin,
	logger log.Logger,
	fileSystem fsys.FileSystem,
	clock clock.Clock,
) api.Response {
	hook := func(op *operations.Operation) error {
		level.Debug(logger).Log("msg", "Running cluster bootstrap operation")

		bootstrapTask := membership.NewBootstrap(
			makeMembershipStateShim(d.State()),
			makeMembershipGatewayShim(d.Gateway()),
			d.Endpoints().NetworkCert(),
			d.NodeConfigSchema(),
			membership.WithFileSystemForBootstrap(fileSystem),
		)
		return bootstrapTask.Run(info.ServerName)
	}
	changer := opChanger{
		d:     d,
		clock: clock,
	}

	op := operations.NewOperation(hook, changer)
	if err := d.Operations().Add(op); err != nil {
		return api.InternalError(err)
	}

	return OperationResponse(op, d.Version())
}

func clusterJoin(
	d api.Daemon,
	info ClusterJoin,
	logger log.Logger,
	fileSystem fsys.FileSystem,
	clock clock.Clock,
) api.Response {
	if info.ClusterCertificate == "" {
		return api.BadRequest(errors.Errorf("cluster certificate not found"))
	}

	address, err := node.HTTPSAddress(d.Node(), d.NodeConfigSchema())
	if err != nil {
		return api.SmartError(err)
	}

	if address == "" {
		if info.ServerAddress == "" {
			return api.BadRequest(errors.Errorf("no core.https_address set on this node"))
		}

		// The user has provided a server address, and not networking was setup
		// on this node, let's do the job and open the port.
		if err := d.Node().Transaction(func(tx *db.NodeTx) error {
			config, err := node.ConfigLoad(tx, d.NodeConfigSchema())
			if err != nil {
				return errors.Wrap(err, "failed to load config")
			}

			if _, err := config.Patch(map[string]interface{}{
				"core.https_address": info.ServerAddress,
			}); err != nil {
				return errors.WithStack(err)
			}
			return nil
		}); err != nil {
			return api.SmartError(err)
		}

		if err := d.Endpoints().NetworkUpdateAddress(info.ServerAddress); err != nil {
			return api.SmartError(err)
		}

		address = info.ServerAddress
	} else {
		if info.ServerAddress != "" && info.ServerAddress != address {
			return api.BadRequest(errors.Errorf("a different core.https_address already exists"))
		}
	}

	// Asynchronously join the cluster.
	varDir := d.State().OS().VarDir()
	hook := func(op *operations.Operation) error {
		level.Info(logger).Log("msg", "Running cluster join operation")

		// Connect to the target cluster node
		client, err := getClusterClient(
			info.ClusterAddress,
			info.ClusterCertificate, info.ClusterKey,
			logger,
		)
		if err != nil {
			return errors.WithStack(err)
		}

		resp, err := clusterAcceptMember(
			client,
			info.ServerName,
			address,
			d.Cluster().SchemaVersion(),
			len(d.APIExtensions()),
		)
		if err != nil {
			return errors.Wrap(err, "failed to request to add node")
		}

		if !bytes.Equal([]byte(info.ClusterKey), resp.PrivateKey) {
			return errors.Wrap(err, "invalid cluster key")
		}

		if err := cert.WriteCert(
			varDir,
			"server",
			[]byte(info.ClusterCertificate),
			resp.PrivateKey,
			cert.WithFileSystem(fileSystem),
		); err != nil {
			return errors.Wrap(err, "failed to save cluster certificate")
		}
		certInfo, err := cert.LoadCert(
			varDir,
			cert.WithFileSystem(fileSystem),
		)
		if err != nil {
			return errors.Wrap(err, "failed to parse cluster certificate")
		}
		if err := d.Endpoints().NetworkUpdateCert(certInfo); err != nil {
			return errors.Wrap(err, "failed to update network certificate")
		}

		// Update local setup and possibly join the raft cluster
		nodes := make([]db.RaftNode, len(resp.RaftNodes))
		for i, node := range resp.RaftNodes {
			nodes[i].ID = node.ID
			nodes[i].Address = node.Address
		}

		joinTask := membership.NewJoin(
			makeMembershipStateShim(d.State()),
			makeMembershipGatewayShim(d.Gateway()),
			d.NodeConfigSchema(),
			membership.WithLoggerForJoin(logger),
		)
		if err := joinTask.Run(certInfo, info.ServerName, nodes); err != nil {
			return errors.Wrap(err, "failed to join")
		}

		return nil
	}

	changer := opChanger{
		d:      d,
		clock:  clock,
		logger: logger,
	}

	op := operations.NewOperation(hook, changer)
	if err := d.Operations().Add(op); err != nil {
		return api.InternalError(err)
	}

	return OperationResponse(op, d.Version())
}

// Perform a request to the /internal/cluster/accept endpoint to check if a new
// mode can be accepted into the cluster and obtain joining information such as
// the cluster private certificate.
func clusterAcceptMember(
	client *client.Client,
	name, address string,
	schema, apiExtensions int,
) (ClusterAcceptResponse, error) {
	req := ClusterAcceptRequest{
		Name:    name,
		Address: address,
		Schema:  schema,
		API:     apiExtensions,
	}
	resp, _, err := client.Query("POST", "/internal/cluster/accept", req, "")
	if err != nil {
		return ClusterAcceptResponse{}, errors.WithStack(err)
	} else if resp.StatusCode != 200 {
		return ClusterAcceptResponse{}, errors.Errorf("invalid status code")
	}

	var info ClusterAcceptResponse
	if err := json.Read(bytes.NewReader(resp.Metadata), &info); err != nil {
		return ClusterAcceptResponse{}, errors.WithStack(err)
	}
	return info, nil
}

// OperationResponse creates an API Response from an operation.
func OperationResponse(op *operations.Operation, version string) api.Response {
	return &operationResponse{
		op:      op,
		version: version,
	}
}

type opChanger struct {
	d      api.Daemon
	logger log.Logger
	clock  clock.Clock
}

func (c opChanger) Change(op operations.Op) {
	msg := struct {
		Type      string    `json:"type"`
		Timestamp time.Time `json:"timestamp"`
		Metadata  ops.Op    `json:"metadata"`
	}{
		Type:      "operation",
		Timestamp: c.clock.Now(),
		Metadata: ops.Op{
			ID:         op.ID,
			Class:      op.Class,
			Status:     op.Status,
			StatusCode: op.StatusCode.Raw(),
			URL:        fmt.Sprintf("/%s%s", c.d.Version(), op.URL),
			Err:        op.Err,
		},
	}

	var res map[string]interface{}
	bytes, err := libjson.Marshal(msg)
	if err != nil {
		level.Error(c.logger).Log("msg", "error marshalling message", "err", err)
		return
	}
	if err := libjson.Unmarshal(bytes, &res); err != nil {
		level.Error(c.logger).Log("msg", "error unmarshalling message", "err", err)
		return
	}

	c.d.EventBroadcaster().Dispatch(res)
}

// Operation response
type operationResponse struct {
	op      *operations.Operation
	version string
}

func (r *operationResponse) Render(w http.ResponseWriter) error {
	if _, err := r.op.Run(); err != nil {
		return errors.WithStack(err)
	}

	res := r.op.Render()
	url := fmt.Sprintf("/%s%s", r.version, res.URL)
	body := client.ResponseRaw{
		Type:       client.AsyncResponse,
		Status:     "Operation Created",
		StatusCode: 200,
		Operation:  url,
		Metadata:   res,
	}

	w.Header().Set("Location", url)
	w.WriteHeader(202)

	return json.Write(w, body, false, log.NewNopLogger())
}

func tryClusterRebalance(d api.Daemon, logger log.Logger) error {
	leader, err := d.Gateway().LeaderAddress()
	if err != nil {
		return errors.Wrap(err, "failed to get current leader node")
	}

	certInfo := d.Endpoints().NetworkCert()
	client, err := getClient(leader, certInfo, logger)
	if err != nil {
		return errors.WithStack(err)
	}
	if _, _, err := client.Query("POST", "/internal/cluster/rebalance", nil, ""); err != nil {
		return errors.Wrap(err, "request to reblance cluster failed")
	}
	return nil
}

func getClient(
	address string,
	certInfo *cert.Info,
	logger log.Logger,
) (*client.Client, error) {
	return client.New(
		net.EnsureHTTPS(address),
		client.WithLogger(log.WithPrefix(logger, "component", "client")),
		client.WithTLSClientCert(string(certInfo.PublicKey())),
		client.WithTLSClientKey(string(certInfo.PrivateKey())),
		client.WithTLSServerCert(string(certInfo.PublicKey())),
	)
}

func getClusterClient(
	address string,
	clusterCertificate, clusterKey string,
	logger log.Logger,
) (*client.Client, error) {
	return client.New(
		net.EnsureHTTPS(address),
		client.WithLogger(log.WithPrefix(logger, "component", "client")),
		client.WithTLSClientCert(clusterCertificate),
		client.WithTLSClientKey(clusterKey),
		client.WithTLSServerCert(clusterCertificate),
		client.WithUserAgent("cluster-notifier"),
	)
}

// ClusterAcceptRequest is a request for the /internal/cluster/accept endpoint.
type ClusterAcceptRequest struct {
	Name    string `json:"name" yaml:"name"`
	Address string `json:"address" yaml:"address"`
	Schema  int    `json:"schema" yaml:"schema"`
	API     int    `json:"api" yaml:"api"`
}

// ClusterAcceptResponse is a Response for the /internal/cluster/accept endpoint.
type ClusterAcceptResponse struct {
	RaftNodes  []RaftNode `json:"raft_nodes" yaml:"raft_nodes"`
	PrivateKey []byte     `json:"private_key" yaml:"private_key"`
}

// RaftNode a node that is part of the raft cluster.
type RaftNode struct {
	ID      int64  `json:"id" yaml:"id"`
	Address string `json:"address" yaml:"address"`
}

// ClusterRaftNode represents a node part of the raft cluster
type ClusterRaftNode struct {
	ServerName string `json:"server_name" yaml:"server_name"`
	URL        string `json:"url" yaml:"url"`
	Database   bool   `json:"database" yaml:"database"`
	Status     string `json:"status" yaml:"status"`
	Message    string `json:"message" yaml:"message"`
}

// ClusterPromoteRequest is a Response for the /internal/cluster/promote endpoint.
type ClusterPromoteRequest struct {
	RaftNodes []RaftNode `json:"raft_nodes" yaml:"raft_nodes"`
}

// ClusterRenameRequest is a request for the /cluster/members/{name} endpoint.
type ClusterRenameRequest struct {
	ServerName string `json:"server_name" yaml:"server_name"`
}

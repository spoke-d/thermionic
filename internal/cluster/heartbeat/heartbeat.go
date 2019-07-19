package heartbeat

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/spoke-d/task"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/net"
)

// DatabaseEndpoint specifies the API endpoint path that gets routed to a dqlite
// server handler for performing SQL queries against the dqlite server running
// on this node.
const DatabaseEndpoint = "/internal/database"

// Task executes a certain function periodically, according to a certain
// schedule.
type Task interface {

	// Every returns a Schedule that always returns the given time interval.
	Every(interval time.Duration, options ...task.EveryOption) task.Schedule
}

// Cluster mediates access to data stored in the cluster dqlite database.
type Cluster interface {
	db.ClusterTransactioner
}

// Node mediates access to the data stored in the node-local SQLite database.
type Node interface {
	db.NodeTransactioner
}

// Gateway mediates access to the dqlite cluster using a gRPC SQL client, and
// possibly runs a dqlite replica on this node (if we're configured to do so).
type Gateway interface {

	// RaftNodes returns information about the nodes that a currently part of
	// the raft cluster, as configured in the raft log. It returns an error if this
	// node is not the leader.
	RaftNodes() ([]db.RaftNode, error)

	// DB returns the database node of the cluster
	DB() Node

	// Cert returns the gateway certificate information
	Cert() *cert.Info

	// Clustered returns if the Gateway is a raft node or is not clustered
	Clustered() bool
}

// CertConfig manages reading a certificate information and convert it into a
// tls.Config.
type CertConfig interface {

	// Read returns a TLS configuration suitable for establishing inter-node
	// network connections using the cluster certificate.
	Read(*cert.Info) (*tls.Config, error)
}

// APIHeartbeatMember contains specific cluster node info.
type APIHeartbeatMember struct {
	ID            int64     // ID field value in nodes table.
	Address       string    // Host and Port of node.
	RaftID        int64     // ID field value in raft_nodes table, zero if non-raft node.
	Raft          bool      // Deprecated, use non-zero RaftID instead to indicate raft node.
	LastHeartbeat time.Time // Last time we received a successful response from node.
	Online        bool      // Calculated from offline threshold and LastHeatbeat time.
	updated       bool      // Has node been updated during this heartbeat run. Not sent to nodes.
}

// APIHeartbeatVersion contains max versions for all nodes in cluster.
type APIHeartbeatVersion struct {
	Schema        int
	APIExtensions int
}

// APIHeartbeat contains data sent to nodes in heartbeat.
type APIHeartbeat struct {
	Members map[int64]APIHeartbeatMember
	Version APIHeartbeatVersion
	Time    time.Time

	// Indicates if heartbeat contains a fresh set of node states.
	// This can be used to indicate to the receiving node that the state is fresh enough to
	// trigger node refresh activies (such as forkdns).
	FullStateList bool
}

// Interval represents the number of seconds to wait between to heartbeat
// rounds.
const Interval = 4

// Heartbeat performs leader-initiated heartbeat checks against all nodes in
// the cluster.
type Heartbeat struct {
	gateway          Gateway
	cluster          Cluster
	task             Task
	certConfig       CertConfig
	databaseEndpoint string
	logger           log.Logger
}

// New creates a new heartbeat with sane defaults
func New(gateway Gateway, cluster Cluster, databaseEndpoint string, options ...Option) *Heartbeat {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Heartbeat{
		gateway:          gateway,
		cluster:          cluster,
		logger:           opts.logger,
		certConfig:       certConfigShim{},
		databaseEndpoint: databaseEndpoint,
		task:             taskShim{},
	}
}

// Run returns a task function that performs leader-initiated heartbeat
// checks against all nodes in the cluster.
//
// It will update the heartbeat timestamp column of the nodes table
// accordingly, and also notify them of the current list of database nodes.
func (h *Heartbeat) Run() (task.Func, task.Schedule) {
	// Since the database APIs are blocking we need to wrap the core logic
	// and run it in a goroutine, so we can abort as soon as the context expires.
	heartbeatWrapper := func(ctx context.Context) {
		ch := make(chan struct{})
		go func() {
			h.run(ctx)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}

	schedule := task.Every(time.Duration(Interval) * time.Second)
	return heartbeatWrapper, schedule
}

func (h *Heartbeat) run(ctx context.Context) {
	if !h.gateway.Clustered() {
		// We're not a raft node or we're not clustered
		return
	}
	level.Debug(h.logger).Log("msg", "Starting heartbeat round")
	raftNodes, err := h.gateway.RaftNodes()
	if errors.Cause(err) == raft.ErrNotLeader {
		level.Debug(h.logger).Log("msg", "Skipping heartbeat since we're not leader")
		return
	}
	if err != nil {
		level.Error(h.logger).Log("msg", "Failed to get current raft nodes", "err", err)
		return
	}
	// Replace the local raft_nodes table immediately because it
	// might miss a row containing ourselves, since we might have
	// been elected leader before the former leader had chance to
	// send us a fresh update through the heartbeat pool.
	level.Debug(h.logger).Log("msg", fmt.Sprintf("Heartbeat updating local raft nodes to %+v", raftNodes))
	if err := h.gateway.DB().Transaction(func(tx *db.NodeTx) error {
		return tx.RaftNodesReplace(raftNodes)
	}); err != nil {
		level.Error(h.logger).Log("msg", "Failed to replace local raft nodes", "err", err)
		return
	}
	var nodes []db.NodeInfo
	var nodeAddress string
	if err := h.cluster.Transaction(func(tx *db.ClusterTx) error {
		var err error
		if nodes, err = tx.Nodes(); err != nil {
			return errors.WithStack(err)
		}
		if nodeAddress, err = tx.NodeAddress(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		level.Error(h.logger).Log("msg", "Failed to get current cluster nodes", "err", err)
		return
	}
	heartbeats := make([]time.Time, len(nodes))

	var mutex sync.Mutex
	var wg sync.WaitGroup

	for i, node := range nodes {

		// Special case the local node
		if node.Address == nodeAddress {
			mutex.Lock()
			heartbeats[i] = time.Now()
			mutex.Unlock()
			continue
		}

		// Parallelize the rest
		wg.Add(1)
		go func(i int, address string) {
			defer wg.Done()

			level.Debug(h.logger).Log("msg", "Sending heartbeat", "address", address)
			if err := h.heartbeatNode(ctx, address, h.gateway.Cert(), raftNodes); err == nil {
				level.Debug(h.logger).Log("msg", "Successful heartbeat", "address", address)

				mutex.Lock()
				heartbeats[i] = time.Now()
				mutex.Unlock()
			} else {
				level.Error(h.logger).Log("msg", "Failed heartbeat", "address", address, "err", err)
			}
		}(i, node.Address)
	}
	wg.Wait()

	// If the context has been cancelled, return immediately.
	if ctx.Err() != nil {
		level.Debug(h.logger).Log("msg", "Aborting heartbeat round")
		return
	}

	if err := h.cluster.Transaction(func(tx *db.ClusterTx) error {
		for i, node := range nodes {
			if heartbeats[i].Equal(time.Time{}) {
				continue
			}
			if err := tx.NodeHeartbeat(node.Address, heartbeats[i]); err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	}); err != nil {
		level.Error(h.logger).Log("msg", "Failed to update heartbeat", "err", err)
	}
	level.Info(h.logger).Log("msg", "Completed heartbeat round")
}

func (h *Heartbeat) heartbeatNode(taskCtx context.Context, address string, cert *cert.Info, raftNodes []db.RaftNode) error {
	level.Debug(h.logger).Log("msg", "Sending heartbeat request", "address", address)

	config, err := h.certConfig.Read(cert)
	if err != nil {
		return errors.WithStack(err)
	}
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: config,
		},
	}

	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(raftNodes); err != nil {
		return errors.WithStack(err)
	}

	url := net.EnsureHTTPS(fmt.Sprintf("%s%s", address, h.databaseEndpoint))
	request, err := http.NewRequest("PUT", url, bytes.NewReader(buffer.Bytes()))
	if err != nil {
		return errors.WithStack(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	request = request.WithContext(ctx)
	request.Close = true // Immediately close the connection after the request is done

	// Perform the request asynchronously, so we can abort it if the task context is done.
	errCh := make(chan error)
	go func() {
		response, err := client.Do(request)
		if err != nil {
			errCh <- errors.Wrap(err, "failed to send HTTP request")
			return
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			errCh <- errors.Errorf("HTTP request failed: %s", response.Status)
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-taskCtx.Done():
		return taskCtx.Err()
	}
}

type taskShim struct{}

func (taskShim) Every(interval time.Duration, options ...task.EveryOption) task.Schedule {
	return task.Every(interval, options...)
}

type certConfigShim struct{}

func (certConfigShim) Read(info *cert.Info) (*tls.Config, error) {
	return cert.TLSClientConfig(info)
}

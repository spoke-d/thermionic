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
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/net"
)

// Current dqlite protocol version.
const dqliteVersion = 1

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
	sync.Mutex

	Members map[int64]APIHeartbeatMember
	Version APIHeartbeatVersion
	Time    time.Time

	// Indicates if heartbeat contains a fresh set of node states.
	// This can be used to indicate to the receiving node that the state is fresh enough to
	// trigger node refresh activies (such as forkdns).
	FullStateList bool

	databaseEndpoint string
	certConfig       CertConfig
	clock            clock.Clock
	logger           log.Logger
}

// Update updates an existing APIHeartbeat struct with the raft and all node states supplied.
// If allNodes provided is an empty set then this is considered a non-full state list.
func (h *APIHeartbeat) Update(fullStateList bool, raftNodes []db.RaftNode, allNodes []db.NodeInfo, offlineThreshold time.Duration) {
	var maxSchemaVersion, maxAPIExtensionsVersion int
	h.Time = h.clock.Now()

	if h.Members == nil {
		h.Members = make(map[int64]APIHeartbeatMember)
	}

	// If we've been supplied a fresh set of node states, this is a full state list.
	h.FullStateList = fullStateList

	raftNodeMap := make(map[string]db.RaftNode)

	// Convert raftNodes to a map keyed on address for lookups later.
	for _, raftNode := range raftNodes {
		raftNodeMap[raftNode.Address] = raftNode
	}

	// Add nodes (overwrites any nodes with same ID in map with fresh data).
	for _, node := range allNodes {
		member := APIHeartbeatMember{
			ID:            node.ID,
			Address:       node.Address,
			LastHeartbeat: node.Heartbeat,
			Online:        !node.Heartbeat.Before(h.clock.Now().Add(-offlineThreshold)),
		}

		if raftNode, exists := raftNodeMap[member.Address]; exists {
			member.Raft = true // Deprecated
			member.RaftID = raftNode.ID
			delete(raftNodeMap, member.Address) // Used to check any remaining later.
		}

		// Add to the members map using the node ID (not the Raft Node ID).
		h.Members[node.ID] = member

		// Keep a record of highest APIExtensions and Schema version seen in all nodes.
		if node.APIExtensions > maxAPIExtensionsVersion {
			maxAPIExtensionsVersion = node.APIExtensions
		}

		if node.Schema > maxSchemaVersion {
			maxSchemaVersion = node.Schema
		}
	}

	h.Version = APIHeartbeatVersion{
		Schema:        maxSchemaVersion,
		APIExtensions: maxAPIExtensionsVersion,
	}

	if len(raftNodeMap) > 0 {
		level.Error(h.logger).Log("msg", "Unaccounted raft node(s) not found in 'nodes' table for heartbeat", "nodes", fmt.Sprintf("%+v", raftNodeMap))
	}

	return
}

// Send sends heartbeat requests to the nodes supplied and updates heartbeat state.
func (h *APIHeartbeat) Send(ctx context.Context, cert *cert.Info, localAddress string, nodes []db.NodeInfo) {
	heartbeatsWg := sync.WaitGroup{}
	sendHeartbeat := func(nodeID int64, address string, heartbeatData *APIHeartbeat) {
		defer heartbeatsWg.Done()

		level.Debug(h.logger).Log("msg", "Sending heartbeat", "address", address)

		err := h.heartbeatNode(ctx, address, cert, heartbeatData)

		if err == nil {
			h.Lock()
			// Ensure only update nodes that exist in Members already.
			hbNode, existing := h.Members[nodeID]
			if !existing {
				return
			}

			hbNode.LastHeartbeat = h.clock.Now()
			hbNode.Online = true
			hbNode.updated = true
			h.Members[nodeID] = hbNode
			h.Unlock()
			level.Debug(h.logger).Log("msg", "Successful heartbeat", "address", address)
		} else {
			level.Error(h.logger).Log("msg", "Failed heartbeat for", "address", address, "err", err)
		}
	}

	for _, node := range nodes {
		// Special case for the local node - just record the time now.
		if node.Address == localAddress {
			h.Lock()
			hbNode := h.Members[node.ID]
			hbNode.LastHeartbeat = h.clock.Now()
			hbNode.Online = true
			hbNode.updated = true
			h.Members[node.ID] = hbNode
			h.Unlock()
			continue
		}

		// Parallelize the rest.
		heartbeatsWg.Add(1)
		go sendHeartbeat(node.ID, node.Address, h)
	}
	heartbeatsWg.Wait()
}

func (h *APIHeartbeat) heartbeatNode(taskCtx context.Context, address string, cert *cert.Info, heartbeatData *APIHeartbeat) error {
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
	if err := json.NewEncoder(&buffer).Encode(heartbeatData); err != nil {
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

	SetDqliteVersionHeader(request)

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
	clock            clock.Clock
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
			h.run(ctx, false)
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

func (h *Heartbeat) run(ctx context.Context, initialHeartbeat bool) {
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
	var offlineThreshold time.Duration
	if err := h.cluster.Transaction(func(tx *db.ClusterTx) error {
		var err error
		if nodes, err = tx.Nodes(); err != nil {
			return errors.WithStack(err)
		}
		if nodeAddress, err = tx.NodeAddress(); err != nil {
			return errors.WithStack(err)
		}
		if offlineThreshold, err = tx.NodeOfflineThreshold(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		level.Error(h.logger).Log("msg", "Failed to get current cluster nodes", "err", err)
		return
	}

	// Cumulative set of node states (will be written back to database once done).
	heartbeats := &APIHeartbeat{
		certConfig:       h.certConfig,
		databaseEndpoint: h.databaseEndpoint,
	}
	cert := h.gateway.Cert()

	// If this leader node hasn't sent a heartbeat recently, then its node state records
	// are likely out of date, this can happen when a node becomes a leader.
	// Send stale set to all nodes in database to get a fresh set of active nodes.
	if initialHeartbeat {
		heartbeats.Update(false, raftNodes, nodes, offlineThreshold)
		heartbeats.Send(ctx, cert, nodeAddress, nodes)

		// We have the latest set of node states now, lets send that state set to all nodes.
		heartbeats.Update(true, raftNodes, nodes, offlineThreshold)
		heartbeats.Send(ctx, cert, nodeAddress, nodes)
	} else {
		heartbeats.Update(true, raftNodes, nodes, offlineThreshold)
		heartbeats.Send(ctx, cert, nodeAddress, nodes)
	}

	// Look for any new node which appeared since sending last heartbeat.
	var currentNodes []db.NodeInfo
	err = h.cluster.Transaction(func(tx *db.ClusterTx) error {
		var err error
		currentNodes, err = tx.Nodes()
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
	if err != nil {
		level.Warn(h.logger).Log("msg", "Failed to get current cluster nodes", "err", err)
		return
	}

	newNodes := []db.NodeInfo{}
	for _, currentNode := range currentNodes {
		existing := false
		for _, node := range nodes {
			if node.Address == currentNode.Address && node.ID == currentNode.ID {
				existing = true
				break
			}
		}

		if !existing {
			// We found a new node
			nodes = append(nodes, currentNode)
			newNodes = append(newNodes, currentNode)
		}
	}

	// If any new nodes found, send heartbeat to just them (with full node state).
	if len(newNodes) > 0 {
		heartbeats.Update(true, raftNodes, nodes, offlineThreshold)
		heartbeats.Send(ctx, cert, nodeAddress, newNodes)
	}

	// If the context has been cancelled, return immediately.
	if ctx.Err() != nil {
		level.Debug(h.logger).Log("msg", "Aborting heartbeat round")
		return
	}

	if err := h.cluster.Transaction(func(tx *db.ClusterTx) error {
		for _, node := range heartbeats.Members {
			if !node.updated {
				continue
			}
			if err := tx.NodeHeartbeat(node.Address, h.clock.Now()); err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	}); err != nil {
		level.Error(h.logger).Log("msg", "Failed to update heartbeat", "err", err)
	}

	level.Info(h.logger).Log("msg", "Completed heartbeat round")
}

type taskShim struct{}

func (taskShim) Every(interval time.Duration, options ...task.EveryOption) task.Schedule {
	return task.Every(interval, options...)
}

type certConfigShim struct{}

func (certConfigShim) Read(info *cert.Info) (*tls.Config, error) {
	return cert.TLSClientConfig(info)
}

// SetDqliteVersionHeader the dqlite version header.
func SetDqliteVersionHeader(request *http.Request) {
	request.Header.Set("X-Dqlite-Version", fmt.Sprintf("%d", dqliteVersion))
}

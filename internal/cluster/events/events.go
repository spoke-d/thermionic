package events

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/task"
	"github.com/spoke-d/thermionic/pkg/client"
	"github.com/spoke-d/thermionic/pkg/events"
)

// Cluster mediates access to data stored in the cluster dqlite database.
type Cluster interface {
	db.ClusterTransactioner
}

// Endpoints are in charge of bringing up and down the HTTP endpoints for
// serving the RESTful API.
type Endpoints interface {

	// NetworkAddress returns the network addresss of the network endpoint, or an
	// empty string if there's no network endpoint
	NetworkAddress() string

	// NetworkCert returns the full TLS certificate information for this endpoint.
	NetworkCert() *cert.Info
}

// EventHook is called when an event is dispatched
type EventHook func(int64, interface{})

// Client represents a way to interact with the server API
type Client interface {

	// Query allows directly querying the API
	Query(string, string, interface{}, string) (*client.Response, string, error)

	// Websocket allows directly connection to API websockets
	Websocket(path string) (*websocket.Conn, error)
}

// EventsSourceProvider creates a new EventsSource
type EventsSourceProvider interface {

	// New creates a new events source for a given address
	Events(string, *cert.Info) (Client, EventsSource, error)
}

// EventsSource returns the raw messages from the server API
type EventsSource interface {

	// GetEvents connects to monitoring interface
	GetEvents() (*events.EventListener, error)
}

// Interval represents the number of seconds to wait between to heartbeat
// rounds.
const Interval = 1

// Events starts a task that continuously monitors the list of cluster nodes and
// maintains a pool of websocket connections against all of them, in order to
// get notified about events.
//
// Whenever an event is received the given hook is invoked.
type Events struct {
	endpoints            Endpoints
	cluster              Cluster
	hook                 EventHook
	eventsSourceProvider EventsSourceProvider
	clock                clock.Clock
	logger               log.Logger
}

// New creates a new event listener with sane defaults
func New(endpoints Endpoints, cluster Cluster, hook EventHook, options ...Option) *Events {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Events{
		endpoints:            endpoints,
		cluster:              cluster,
		hook:                 hook,
		eventsSourceProvider: opts.eventsSourceProvider,
		clock:                opts.clock,
		logger:               opts.logger,
	}
}

// Run returns a task function that performs event listener
// checks against all nodes in the cluster.
func (e *Events) Run() (task.Func, task.Schedule) {
	listeners := make(map[int64]*events.EventListener)

	// Update our pool of event listeners. Since database queries are
	// blocking, we spawn the actual logic in a goroutine, to abort
	// immediately when we receive the stop signal.
	eventsWrapper := func(ctx context.Context) {
		ch := make(chan struct{})
		go func() {
			e.updateListeners(listeners)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}

	schedule := task.Every(time.Duration(Interval) * time.Second)
	return eventsWrapper, schedule
}

func (e *Events) updateListeners(listeners map[int64]*events.EventListener) {
	// Get the current cluster nodes.
	var nodes []db.NodeInfo
	var offlineThreshold time.Duration

	if err := e.cluster.Transaction(func(tx *db.ClusterTx) error {
		var err error
		if nodes, err = tx.Nodes(); err != nil {
			return errors.WithStack(err)
		}

		if offlineThreshold, err = tx.NodeOfflineThreshold(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		level.Warn(e.logger).Log("msg", "failed to get current cluster nodes", "err", err)
		return
	}
	if len(nodes) == 1 {
		// Either we're not clustered or this is a single-node cluster
		return
	}

	address := e.endpoints.NetworkAddress()

	ids := make([]int, len(nodes))
	for i, node := range nodes {
		ids[i] = int(node.ID)

		// Don't bother trying to connect to offline nodes, or to ourselves.
		if node.IsOffline(e.clock, offlineThreshold) || node.Address == address {
			continue
		}

		_, ok := listeners[node.ID]
		// The node has already a listener associated to it.
		if ok {
			// Double check that the listener is still
			// connected. If it is, just move on, other
			// we'll try to connect again.
			if listeners[node.ID].IsActive() {
				continue
			}
			delete(listeners, node.ID)
		}

		_, source, err := e.eventsSourceProvider.Events(node.Address, e.endpoints.NetworkCert())
		if err != nil {
			continue
		}
		listener, err := source.GetEvents()
		if err != nil {
			level.Warn(e.logger).Log("msg", "failed to get events from node", "address", node.Address, "err", err)
			continue
		}
		level.Debug(e.logger).Log("msg", "listening for events on node", "address", node.Address)

		listener.AddHandler(nil, func(event interface{}) {
			e.hook(node.ID, event)
		})
		listeners[node.ID] = listener
	}

	for id, listener := range listeners {
		if !contains(ids, int(id)) {
			listener.Disconnect()
			delete(listeners, id)
		}
	}
}

func contains(ids []int, id int) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

type eventsSourceProvider struct{}

// Connect configures the client with the correct parameters for
// node-to-node communication.
func (eventsSourceProvider) Events(address string, cert *cert.Info) (Client, EventsSource, error) {
	rawClient, err := client.New(
		fmt.Sprintf("https://%s", address),
		client.WithTLSServerCert(string(cert.PublicKey())),
		client.WithTLSClientCert(string(cert.PublicKey())),
		client.WithTLSClientKey(string(cert.PrivateKey())),
		client.WithUserAgent("cluster-notifier"),
	)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return rawClient, events.NewEvents(rawClient), nil
}

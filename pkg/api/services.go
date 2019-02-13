package api

import (
	"context"
	"crypto/x509"
	"net/http"

	"github.com/go-kit/kit/log/level"

	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
)

// DaemonServiceRouter creates a wrapper of a underlying router, then allows
// services to be added and handled.
type DaemonServiceRouter struct {
	*ServiceRouter
	d Daemon
}

// NewDaemonServiceRouter creates a ServiceRouter with sane defaults
func NewDaemonServiceRouter(
	d Daemon,
	mux *mux.Router,
	clientCerts, clusterCerts func() []x509.Certificate,
	logger log.Logger,
) *DaemonServiceRouter {
	return &DaemonServiceRouter{
		ServiceRouter: &ServiceRouter{
			setupChan: func() <-chan struct{} {
				return d.SetupChan()
			},
			context: func() context.Context {
				return context.WithValue(context.Background(), DaemonKey, d)
			},
			validNonce: func(nonce string) bool {
				return d.Nonce() == nonce
			},
			mux:          mux,
			clientCerts:  clientCerts,
			clusterCerts: clusterCerts,
			logger:       logger,
		},
		d: d,
	}
}

// DaemonRestServer creates an http.Server capable of handling requests against the
// REST API endpoint.
func DaemonRestServer(
	d Daemon,
	services, internalServices []Service,
	options ...Option,
) (*http.Server, SchedulerTask, error) {
	opts := newOptions()
	opts.services = services
	opts.internalServices = internalServices
	for _, option := range options {
		option(opts)
	}

	router := func(mux *mux.Router) httpRouteHandler {
		return NewDaemonServiceRouter(d, mux, d.ClientCerts, d.ClusterCerts, opts.logger)
	}
	server := makeRestServer(
		router, d,
		services, internalServices,
		d.Gateway().HandlerFuncs(),
		opts,
	)
	scheduler := makeRestServerScheduler(
		server,
		d,
		opts.logger,
	)
	go func() {
		select {
		case <-d.SetupChan():
		}
		config, err := scheduler.readConfig()
		if err != nil {
			level.Error(opts.logger).Log("msg", "error reading config", "err", err)
			return
		}
		server.Init(config)
	}()

	return &http.Server{
		Handler: server,
	}, scheduler, nil
}

// DiscoveryServiceRouter creates a wrapper of a underlying router, then allows
// services to be added and handled.
type DiscoveryServiceRouter struct {
	*ServiceRouter
	d Discovery
}

// NewDiscoveryServiceRouter creates a ServiceRouter with sane defaults
func NewDiscoveryServiceRouter(
	d Discovery,
	mux *mux.Router,
	clientCerts, clusterCerts func() []x509.Certificate,
	logger log.Logger,
) *DiscoveryServiceRouter {
	return &DiscoveryServiceRouter{
		ServiceRouter: &ServiceRouter{
			setupChan: func() <-chan struct{} {
				return d.SetupChan()
			},
			context: func() context.Context {
				return context.WithValue(context.Background(), DiscoveryKey, d)
			},
			validNonce: func(nonce string) bool {
				return false
			},
			mux:          mux,
			clientCerts:  clientCerts,
			clusterCerts: clusterCerts,
			logger:       logger,
		},
		d: d,
	}
}

// DiscoveryRestServer creates an http.Server capable of handling requests against the
// REST API endpoint.
func DiscoveryRestServer(
	d Discovery,
	config *clusterconfig.ReadOnlyConfig,
	services, internalServices []Service,
	options ...Option,
) (*http.Server, error) {
	opts := newOptions()
	opts.services = services
	opts.internalServices = internalServices
	for _, option := range options {
		option(opts)
	}

	router := func(mux *mux.Router) httpRouteHandler {
		return NewDiscoveryServiceRouter(d, mux, d.ClientCerts, d.ClusterCerts, opts.logger)
	}
	server := makeRestServer(
		router, d,
		services, internalServices,
		map[string]http.HandlerFunc{},
		opts,
	)
	server.Init(config)

	return &http.Server{
		Handler: server,
	}, nil
}

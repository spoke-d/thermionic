package endpoints

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/retrier"
	"github.com/spoke-d/thermionic/internal/tomb"
)

// DefaultPort defines the default port for the http server
const DefaultPort = "8080"

// Numeric code identifying a specific API endpoint type.
type kind int

// Numeric codes identifying the various endpoints.
const (
	network kind = iota
	pprof
)

func (k kind) Description() string {
	switch k {
	case network:
		return "TCP socket"
	case pprof:
		return "pprof socket"
	default:
		return "unknown"
	}
}

// Server is a point of use interface for a http.Server
type Server interface {
	// Server traffic from the listener.
	Serve(net.Listener) error
}

// Endpoints are in charge of bringing up and down the HTTP endpoints for
// serving the RESTful API.
//
// When it starts up, they start listen to the appropriate sockets and attach
// the relevant HTTP handlers to them. When it shuts down they close all
// sockets.
type Endpoints struct {
	tomb      *tomb.Tomb
	mutex     sync.RWMutex
	listeners map[kind]net.Listener
	servers   map[kind]Server
	cert      *cert.Info
	logger    log.Logger
	sleeper   clock.Sleeper
}

// New creates Endpoints with sane defaults
func New(restServer Server, cert *cert.Info, options ...Option) *Endpoints {
	opts := newOptions()
	opts.restServer = restServer
	opts.cert = cert
	for _, option := range options {
		option(opts)
	}

	return &Endpoints{
		tomb: tomb.New(),
		listeners: map[kind]net.Listener{
			network: networkCreateListener(opts.networkAddress, cert, opts.logger),
			pprof:   pprofCreateListener(opts.debugAddress, opts.logger),
		},
		servers: map[kind]Server{
			network: opts.restServer,
			pprof:   pprofCreateServer(),
		},
		cert:    opts.cert,
		logger:  opts.logger,
		sleeper: opts.sleeper,
	}
}

// Up brings down all endpoints and stops serving HTTP requests.
func (e *Endpoints) Up() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	level.Info(e.logger).Log("msg", "REST API daemon")
	if err := e.serveHTTP(network); err != nil {
		return errors.WithStack(err)
	}
	if err := e.serveHTTP(pprof); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// Down brings down all endpoints and stops serving HTTP requests.
func (e *Endpoints) Down() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	for _, v := range []kind{network, pprof} {
		if e.listeners[v] != nil {
			level.Info(e.logger).Log("msg", "Stopping API handler", "kind", v.Description())
			err := e.closeListener(v)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	if e.tomb != nil {
		e.tomb.Kill(nil)
		e.tomb.Wait()
	}

	return nil
}

// NetworkAddress returns the network addresses of the network endpoint, or an
// empty string if there's no network endpoint
func (e *Endpoints) NetworkAddress() string {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	listener := e.listeners[network]
	if listener == nil {
		return ""
	}
	return listener.Addr().String()
}

// PprofAddress returns the network addresss of the pprof endpoint, or an empty
// string if there's no pprof endpoint
func (e *Endpoints) PprofAddress() string {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	listener := e.listeners[pprof]
	if listener == nil {
		return ""
	}
	return listener.Addr().String()
}

// NetworkPublicKey returns the public key of the TLS certificate used by the
// network endpoint.
func (e *Endpoints) NetworkPublicKey() []byte {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return e.cert.PublicKey()
}

// NetworkPrivateKey returns the private key of the TLS certificate used by the
// network endpoint.
func (e *Endpoints) NetworkPrivateKey() []byte {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return e.cert.PrivateKey()
}

// NetworkCert returns the full TLS certificate information for this endpoint.
func (e *Endpoints) NetworkCert() *cert.Info {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return e.cert
}

// NetworkUpdateCert updates the TLS keypair and CA used by the network
// endpoint.
//
// If the network endpoint is active, in-flight requests will continue using
// the old certificate, and only new requests will use the new one.
func (e *Endpoints) NetworkUpdateCert(cert *cert.Info) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.cert = cert
	if listener, ok := e.listeners[network]; ok {
		listener.(*networkListener).Config(cert)
	}
	return nil
}

// NetworkUpdateAddress updates the address for the network endpoint, shutting
// it down and restarting it.
func (e *Endpoints) NetworkUpdateAddress(address string) error {
	return e.newListener(network, e.NetworkAddress(), address)
}

// PprofUpdateAddress updates the address for the pprof endpoint, shutting
// it down and restarting it.
func (e *Endpoints) PprofUpdateAddress(address string) error {
	return e.newListener(pprof, e.PprofAddress(), address)
}

// Start an HTTP server for the endpoint associated with the given code.
func (e *Endpoints) serveHTTP(kind kind) error {
	listener := e.listeners[kind]

	if listener == nil {
		return nil
	}

	message := fmt.Sprintf(" - binding %s", kind.Description())
	level.Info(e.logger).Log("msg", message, "address", listener.Addr())

	server := e.servers[kind]

	if err := e.tomb.Go(func() error {
		return server.Serve(listener)
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Stop the HTTP server of the endpoint associated with the given code. The
// associated socket will be shutdown too.
func (e *Endpoints) closeListener(kind kind) error {
	listener := e.listeners[kind]
	if listener == nil {
		return nil
	}
	delete(e.listeners, kind)

	level.Info(e.logger).Log("msg", " - closing socket", "address", listener.Addr())

	return listener.Close()
}

func (e *Endpoints) newListener(k kind, oldAddress, address string) error {
	if address != "" {
		address = canonicalNetworkAddress(address)
	}

	if address == oldAddress {
		return nil
	}

	level.Info(e.logger).Log("msg", " - updating address", "old-address", oldAddress, "address", address)

	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Close the previous socket
	e.closeListener(k)
	if address == "" {
		return nil
	}

	// Attempt to setup the new listening socket
	getListener := func(address string) (net.Listener, error) {
		var listener net.Listener
		retry := retrier.New(e.sleeper, 10, time.Millisecond*100)
		if err := retry.Run(func() error {
			var err error
			listener, err = net.Listen("tcp", address)
			return err
		}); err != nil {
			return nil, errors.WithStack(err)
		}
		return listener, nil
	}

	listener, err := getListener(address)
	if err != nil {
		// Attempt to revert to the previous address
		if listener, err := getListener(oldAddress); err != nil {
			e.listeners[k] = networkTLSListener(listener, e.cert, e.logger)
			e.serveHTTP(k)
		}
		return errors.WithStack(err)
	}

	e.listeners[k] = networkTLSListener(listener, e.cert, e.logger)
	return e.serveHTTP(k)
}

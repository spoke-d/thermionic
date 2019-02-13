package api

import (
	"context"
	"crypto/x509"
	"fmt"
	"net/http"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// Service represents a endpoint that can perform http actions upon
type Service interface {

	// Get handles GET requests
	Get(context.Context, *http.Request) Response

	// Put handles PUT requests
	Put(context.Context, *http.Request) Response

	// Post handles POST requests
	Post(context.Context, *http.Request) Response

	// Delete handles DELETE requests
	Delete(context.Context, *http.Request) Response

	// Patch handles PATCH requests
	Patch(context.Context, *http.Request) Response

	// Name returns the serialisable service name.
	// The name has to conform to RFC 3986
	Name() string
}

// DefaultService creates a default service that just returns not
// implemented errors
type DefaultService struct{}

// Get handles GET requests, but always returns NotImplemented
func (DefaultService) Get(d context.Context, req *http.Request) Response {
	return NotImplemented(nil)
}

// Put handles PUT requests, but always returns NotImplemented
func (DefaultService) Put(d context.Context, req *http.Request) Response {
	return NotImplemented(nil)
}

// Post handles POST requests, but always returns NotImplemented
func (DefaultService) Post(d context.Context, req *http.Request) Response {
	return NotImplemented(nil)
}

// Delete handles DELETE requests, but always returns NotImplemented
func (DefaultService) Delete(d context.Context, req *http.Request) Response {
	return NotImplemented(nil)
}

// Patch handles PATCH requests, but always returns NotImplemented
func (DefaultService) Patch(d context.Context, req *http.Request) Response {
	return NotImplemented(nil)
}

// ContextKey defines a key that can be used to identify values within a
// context value.
type ContextKey string

const (
	// DaemonKey represents a way to identify a daemon in a context
	DaemonKey ContextKey = "daemon"

	// DiscoveryKey represents a way to identify a service in a context
	DiscoveryKey ContextKey = "discovery"
)

// GetDaemon returns the Daemon from the context or return an error
func GetDaemon(ctx context.Context) (Daemon, error) {
	d := ctx.Value(DaemonKey)
	if d == nil {
		return nil, errors.Errorf("daemon not found")
	}
	return d.(Daemon), nil
}

// GetDiscovery returns the Discovery from the context or return an error
func GetDiscovery(ctx context.Context) (Discovery, error) {
	d := ctx.Value(DiscoveryKey)
	if d == nil {
		return nil, errors.Errorf("discovery not found")
	}
	return d.(Discovery), nil
}

// ServiceRouter creates a wrapper of a underlying router, then allows
// services to be added and handled.
type ServiceRouter struct {
	setupChan    func() <-chan struct{}
	context      func() context.Context
	validNonce   func(string) bool
	mux          *mux.Router
	clientCerts  func() []x509.Certificate
	clusterCerts func() []x509.Certificate
	logger       log.Logger
}

// Add a Service to the DaemonServiceRouter with a prefix (1.0/internal)
func (s *ServiceRouter) Add(prefix string, service Service) {
	uri := serviceURI(prefix, service)

	level.Debug(s.logger).Log("msg", "Registering service", "uri", uri, "name", service.Name())

	s.mux.HandleFunc(uri, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Block public API requests until we're done with basic
		// initialization tasks, such setting up the cluster database.
		select {
		case <-s.setupChan():
		default:
			response := Unavailable(fmt.Errorf("daemon setup in progress"))
			response.Render(w)
			return
		}

		if s.validNonce(r.FormValue("nonce")) {
			level.Debug(s.logger).Log("msg", "valid nonce given")
		} else if err := s.validateClient(r); err != nil {
			level.Error(s.logger).Log("msg", "rejecting request from untrusted client", "err", err)
			Forbidden(nil).Render(w)
			return
		}

		ctx := s.context()

		var resp Response
		switch r.Method {
		case "GET":
			resp = service.Get(ctx, r)
		case "PUT":
			resp = service.Put(ctx, r)
		case "POST":
			resp = service.Post(ctx, r)
		case "DELETE":
			resp = service.Delete(ctx, r)
		case "PATCH":
			resp = service.Patch(ctx, r)
		default:
			resp = NotFound(errors.Errorf("method %q not found for %q", r.Method, uri))
		}

		if err := resp.Render(w); err != nil {
			if internalErr := InternalError(err).Render(w); internalErr != nil {
				level.Error(s.logger).Log("msg", "failed writing error for error, giving up", "method", r.Method, "uri", uri)
			}
		}
	})
}

func (s *ServiceRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO Gather metrics per route
	s.mux.ServeHTTP(w, r)
}

// Check whether the request comes from a trusted client.
func (s *ServiceRouter) validateClient(r *http.Request) error {
	// Check the cluster certificate first, so we return an error if the
	// notification header is set but the client is not presenting the
	// cluster certificate (iow this request does not appear to come from a
	// cluster node).
	if r.TLS != nil {
		clusterCerts := s.clusterCerts()
		for _, v := range r.TLS.PeerCertificates {
			if cert.CheckTrustState(*v, clusterCerts) {
				return nil
			}
		}
	}
	if IsClusterNotification(r) {
		return errors.New("cluster notification not using cluster certificate")
	}
	// ignore unix socket requests.
	if r.RemoteAddr == "@" {
		return nil
	}
	if r.TLS == nil {
		return errors.New("no TLS")
	}
	clientCerts := s.clientCerts()
	for _, v := range r.TLS.PeerCertificates {
		if cert.CheckTrustState(*v, clientCerts) {
			return nil
		}
	}
	return errors.New("unauthorized")
}

// IsClusterNotification returns true if this an API request coming from a
// cluster node that is notifying us of some user-initiated API request that
// needs some action to be taken on this node as well.
func IsClusterNotification(r *http.Request) bool {
	return r.Header.Get("User-Agent") == "cluster-notifier"
}

func serviceURI(version string, s Service) string {
	if name := s.Name(); name != "" {
		return fmt.Sprintf("/%s/%s", version, name)
	}
	return fmt.Sprintf("/%s", version)
}

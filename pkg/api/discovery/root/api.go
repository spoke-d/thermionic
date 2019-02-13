package root

import (
	"context"
	"net/http"
	"os"

	"github.com/spoke-d/thermionic/internal/cert"
	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/net"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/go-kit/kit/log"
)

// API defines a query API
type API struct {
	api.DefaultService
	config *clusterconfig.ReadOnlyConfig
	logger log.Logger
}

// NewAPI creates a API with sane defaults
func NewAPI(config *clusterconfig.ReadOnlyConfig, options ...Option) *API {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &API{
		config: config,
		logger: opts.logger,
	}
}

// Name returns the API name
func (a *API) Name() string {
	return ""
}

// Get defines a service for calling "GET" method and returns a response.
func (a *API) Get(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDiscovery(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	address, err := a.config.DiscoveryAddress()
	if err != nil {
		return api.InternalError(err)
	}

	addresses, err := net.ListenAddresses(address)
	if err != nil {
		return api.InternalError(err)
	}

	// When clustered, use the node name, otherwise use the hostname.
	serverName, err := os.Hostname()
	if err != nil {
		return api.SmartError(err)
	}

	var certificateFingerprint string
	certificate := string(d.Endpoints().NetworkPublicKey())
	if certificate != "" {
		if certificateFingerprint, err = cert.FingerprintStr(certificate); err != nil {
			return api.InternalError(err)
		}
	}

	server := Server{
		Environment: Environment{
			Addresses:              addresses,
			Certificate:            certificate,
			CertificateFingerprint: certificateFingerprint,
			Server:                 "therm",
			ServerPid:              os.Getpid(),
			ServerVersion:          d.Version(),
			ServerName:             serverName,
		},
		Config: make(map[string]interface{}),
	}
	return api.SyncResponseETag(true, server, server.Config)
}

// Server represents the structure for the server
type Server struct {
	Environment Environment            `json:"environment" yaml:"environment"`
	Config      map[string]interface{} `json:"config" yaml:"config"`
}

// Environment defines the server environment for the daemon
type Environment struct {
	Addresses              []string `json:"addresses" yaml:"addresses"`
	Certificate            string   `json:"certificate" yaml:"certificate"`
	CertificateFingerprint string   `json:"certificate_fingerprint" yaml:"certificate_fingerprint"`
	Server                 string   `json:"server" yaml:"server"`
	ServerPid              int      `json:"server_pid" yaml:"server_pid"`
	ServerVersion          string   `json:"server_version" yaml:"server_version"`
	ServerName             string   `json:"server_name" yaml:"server_name"`
}

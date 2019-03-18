package root

import (
	"context"
	"net/http"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster"
	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/etag"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/internal/net"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/pkg/api"
)

// API defines a query API
type API struct {
	api.DefaultService
	nodeConfigSchema config.Schema
	logger           log.Logger
}

// NewAPI creates a API with sane defaults
func NewAPI(nodeConfigSchema config.Schema, options ...Option) *API {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &API{
		nodeConfigSchema: nodeConfigSchema,
		logger:           opts.logger,
	}
}

// Name returns the API name
func (a *API) Name() string {
	return ""
}

// Get defines a service for calling "GET" method and returns a response.
func (a *API) Get(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	// trusted defines if the request is known to the daemon
	var trusted bool
	nonce := req.FormValue("nonce")
	if d.Nonce() == nonce {
		trusted = true
	}

	address, err := node.HTTPSAddress(d.Node(), a.nodeConfigSchema)
	if err != nil {
		return api.InternalError(err)
	}
	addresses, err := net.ListenAddresses(address)
	if err != nil {
		return api.InternalError(err)
	}

	clustered, err := cluster.Enabled(d.Node())
	if err != nil {
		return api.SmartError(err)
	}

	// When clustered, use the node name, otherwise use the hostname.
	var serverName string
	if clustered {
		if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
			var err error
			serverName, err = tx.NodeName()
			return err
		}); err != nil {
			return api.SmartError(err)
		}
	} else {
		if serverName, err = os.Hostname(); err != nil {
			return api.SmartError(err)
		}
	}

	var certificateFingerprint string
	certificate := string(d.Endpoints().NetworkPublicKey())
	if certificate != "" {
		if certificateFingerprint, err = cert.FingerprintStr(certificate); err != nil {
			return api.InternalError(err)
		}
	}

	config, err := readConfig(d.Cluster(), d.Node(), d.ClusterConfigSchema(), d.NodeConfigSchema())
	if err != nil {
		return api.InternalError(err)
	}

	server := Server{
		Environment: Environment{
			Addresses:              addresses,
			Certificate:            certificate,
			CertificateFingerprint: certificateFingerprint,
			Server:                 "therm",
			ServerPid:              os.Getpid(),
			ServerVersion:          d.Version(),
			ServerClustered:        clustered,
			ServerName:             serverName,
		},
		Config: config,
	}
	if trusted {
		key := string(d.Endpoints().NetworkPrivateKey())
		server.Environment.CertificateKey = key
	}
	return api.SyncResponseETag(true, server, server.Config)
}

// Put defines a service for calling "PUT" method and returns a response.
func (a *API) Put(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var info ServerUpdate
	if err := json.Read(req.Body, &info); err != nil {
		return api.BadRequest(err)
	}

	if api.IsClusterNotification(req) {
		level.Debug(a.logger).Log("msg", "Handling config change notification")

		changed := make(map[string]string)
		for key, value := range info.Config {
			if v, ok := value.(string); ok {
				changed[key] = v
			}
		}

		var config *clusterconfig.Config
		if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
			var err error
			config, err = clusterconfig.Load(tx, d.ClusterConfigSchema())
			return err
		}); err != nil {
			return api.SmartError(err)
		}

		if err := updateClusterTriggers(d, changed, config); err != nil {
			return api.SmartError(err)
		}
		return api.EmptySyncResponse()
	}

	render, err := readConfig(d.Cluster(), d.Node(), d.ClusterConfigSchema(), d.NodeConfigSchema())
	if err != nil {
		return api.SmartError(err)
	}
	if err := etag.Check(req, render); err != nil {
		return api.PreconditionFailed(err)
	}
	return update(d, info, false)
}

// Patch defines a service for calling "PATCH" method and returns a response.
func (a *API) Patch(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var info ServerUpdate
	if err := json.Read(req.Body, &info); err != nil {
		return api.BadRequest(err)
	}
	if info.Config == nil {
		return api.EmptySyncResponse()
	}

	render, err := readConfig(d.Cluster(), d.Node(), d.ClusterConfigSchema(), d.NodeConfigSchema())
	if err != nil {
		return api.SmartError(err)
	}
	if err := etag.Check(req, render); err != nil {
		return api.PreconditionFailed(err)
	}
	return update(d, info, true)
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
	CertificateKey         string   `json:"certificate_key,omitempty" yaml:"certificate_key,omitempty"`
	Server                 string   `json:"server" yaml:"server"`
	ServerPid              int      `json:"server_pid" yaml:"server_pid"`
	ServerVersion          string   `json:"server_version" yaml:"server_version"`
	ServerClustered        bool     `json:"server_clustered" yaml:"server_clustered"`
	ServerName             string   `json:"server_name" yaml:"server_name"`
}

// ServerUpdate represents what can be changed when updating the server
// information
type ServerUpdate struct {
	Config map[string]interface{} `json:"config" yaml:"config"`
}

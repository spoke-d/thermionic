package client

import (
	"bytes"
	"fmt"

	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/pkg/api/daemon/root"
	"github.com/spoke-d/thermionic/pkg/client"
	"github.com/pkg/errors"
)

// Info represents a way of interacting with the daemon API, which is
// responsible for getting the information from the daemon.
type Info struct {
	client *Client
}

// Get returns the information from the daemon API
func (i *Info) Get() (InfoResult, error) {
	var param string
	if nonce := i.client.nonce; nonce != "" {
		param = fmt.Sprintf("?nonce=%s", nonce)
	}

	var result InfoResult
	if err := i.client.exec("GET", fmt.Sprintf("/1.0%s", param), nil, "", func(response *client.Response, meta Metadata) error {
		var server root.Server
		if err := json.Read(bytes.NewReader(response.Metadata), &server); err != nil {
			return errors.Wrap(err, "error parsing result")
		}

		config := make(map[string]interface{}, len(server.Config))
		for k, v := range server.Config {
			config[k] = v
		}

		result = InfoResult{
			Environment: Environment{
				Addresses:              server.Environment.Addresses,
				Certificate:            server.Environment.Certificate,
				CertificateFingerprint: server.Environment.CertificateFingerprint,
				CertificateKey:         server.Environment.CertificateKey,
				Server:                 server.Environment.Server,
				ServerPid:              server.Environment.ServerPid,
				ServerVersion:          server.Environment.ServerVersion,
				ServerClustered:        server.Environment.ServerClustered,
				ServerName:             server.Environment.ServerName,
			},
			Config: config,
		}
		return nil
	}); err != nil {
		return result, errors.WithStack(err)
	}
	return result, nil
}

// InfoResult contains the result of querying the daemon information API
type InfoResult struct {
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

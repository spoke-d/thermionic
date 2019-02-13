package client

import (
	"bytes"

	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/pkg/api/daemon/services"
	"github.com/spoke-d/thermionic/pkg/client"
	"github.com/pkg/errors"
)

// Discovery represents a way of interacting with the daemon API, which is
// responsible for getting the discovery information from the daemon.
type Discovery struct {
	client *Client
}

// Services returns the service information result from the discovery services
// API
func (d *Discovery) Services() *Services {
	return &Services{
		client: d.client,
	}
}

// Services represents a way of interacting with the daemon API, which is
// responsible for getting the discovery service information from the daemon.
type Services struct {
	client *Client
}

// List all the possible services that are attached to the discovery API
func (s *Services) List() ([]ServiceResult, error) {
	var result []ServiceResult
	if err := s.client.exec("GET", "/1.0/services/members", nil, "", func(response *client.Response, meta Metadata) error {
		var services []services.ServiceResult
		if err := json.Read(bytes.NewReader(response.Metadata), &services); err != nil {
			return errors.Wrap(err, "error parsing result")
		}
		for _, v := range services {
			result = append(result, ServiceResult{
				ServiceNode: ServiceNode{
					ServerName:    v.ServerName,
					ServerAddress: v.ServerAddress,
					DaemonAddress: v.DaemonAddress,
					DaemonNonce:   v.DaemonNonce,
				},
				Status:  v.Status,
				Message: v.Message,
			})
		}

		return nil
	}); err != nil {
		return result, errors.WithStack(err)
	}
	return result, nil
}

// Add adds a service node to the discovery API
func (s *Services) Add(nodes []ServiceNode) error {
	data := make([]services.ServiceNode, len(nodes))
	for k, v := range nodes {
		data[k] = services.ServiceNode{
			ServerName:    v.ServerName,
			ServerAddress: v.ServerAddress,
			DaemonAddress: v.DaemonAddress,
			DaemonNonce:   v.DaemonNonce,
		}
	}
	if err := s.client.exec("POST", "/1.0/services/members", data, "", nopReadFn); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Delete removes a service node to the discovery API
func (s *Services) Delete(nodes []ServiceNode) error {
	data := make([]services.ServiceNode, len(nodes))
	for k, v := range nodes {
		data[k] = services.ServiceNode{
			ServerName:    v.ServerName,
			ServerAddress: v.ServerAddress,
			DaemonAddress: v.DaemonAddress,
		}
	}
	if err := s.client.exec("DELETE", "/1.0/services/members", data, "", nopReadFn); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// ServiceResult represents a node part of the raft cluster
type ServiceResult struct {
	ServiceNode `json:",inline" yaml:",inline"`
	Status      string `json:"status" yaml:"status"`
	Message     string `json:"message" yaml:"message"`
}

// ServiceNode represents a request to update the node for the raft
// cluster
type ServiceNode struct {
	ServerName    string `json:"server_name" yaml:"server_name"`
	ServerAddress string `json:"server_address" yaml:"server_address"`
	DaemonAddress string `json:"daemon_address" yaml:"daemon_address"`
	DaemonNonce   string `json:"daemon_nonce" yaml:"daemon_nonce"`
}

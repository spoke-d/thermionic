package client

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/net"
	"github.com/spoke-d/thermionic/pkg/client"
)

// Client describes a very simple client for connecting to a daemon rest API
// The client expects that certificates are already existing and you can just
// pass the values through the constructor.
// All requests will be made over HTTPS, it's currently not possible to send
// any requests in an insecure, downgraded manor.
type Client struct {
	client *client.Client
	nonce  string
	logger log.Logger
}

// New creates a Client using the address and certificates.
func New(address, serverCert, clientCert, clientKey string, options ...Option) (*Client, error) {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	client, err := client.New(
		net.EnsureHTTPS(address),
		client.WithTLSServerCert(serverCert),
		client.WithTLSClientCert(clientCert),
		client.WithTLSClientKey(clientKey),
		client.WithLogger(opts.logger),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Client{
		client: client,
		logger: opts.logger,
	}, nil
}

// NewUntrusted creates a new Client used to connect to hosts in a untrusted
// manor.
func NewUntrusted(address, nonce string, options ...Option) (*Client, error) {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	client, err := client.New(
		net.EnsureHTTPS(address),
		client.WithInsecureSkipVerify(true),
		client.WithLogger(opts.logger),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Client{
		client: client,
		nonce:  nonce,
		logger: opts.logger,
	}, nil
}

// Info returns the information result from the daemon API
func (c *Client) Info() *Info {
	return &Info{
		client: c,
	}
}

// Cluster returns an API leaf for interacting with the cluster API
func (c *Client) Cluster() *Cluster {
	return &Cluster{
		client: c,
	}
}

// Discovery returns an API leaf for interacting with the discovery API
func (c *Client) Discovery() *Discovery {
	return &Discovery{
		client: c,
	}
}

// Query represents a raw query to the daemon API
// This method is **deprecated** should not be used, it's here as a way to
// transition to the new client
func (c *Client) Query(method, path string, data interface{}, etag string) (*client.Response, string, error) {
	return c.client.Query(method, path, data, etag)
}

// RawClient returns the underlying client that services this client.
// This method is **deprecated** should not be used, it's here as a way to
// transition to the new client
func (c *Client) RawClient() *client.Client {
	return c.client
}

func nopReadFn(*client.Response, Metadata) error {
	return nil
}

func (c *Client) exec(
	method, path string,
	body interface{},
	etag string,
	fn func(*client.Response, Metadata) error,
) error {
	began := time.Now()
	response, etag, err := c.client.Query(method, path, body, etag)
	if err != nil {
		return errors.Wrap(err, "error requesting")
	} else if response.StatusCode != 200 {
		return errors.Errorf("invalid status code %d", response.StatusCode)
	}
	return fn(response, Metadata{
		ETag:     etag,
		Duration: time.Since(began),
	})
}

// Metadata holds the metadata for each result.
type Metadata struct {
	ETag     string
	Duration time.Duration
}

package operations

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/operations"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/spoke-d/thermionic/pkg/client"
)

// Status represents operational status
type Status = operations.Status

// Op defines a very lightweight operation
type Op struct {
	ID         string `json:"id" yaml:"id"`
	URL        string `json:"url" yaml:"url"`
	Class      string `json:"class" yaml:"class"`
	Status     string `json:"status" yaml:"status"`
	StatusCode int    `json:"status_code" yaml:"status_code"`
	Err        string `json:"err" yaml:"err"`
}

// Operations defines an interface for interacting with a series of operations
type Operations interface {

	// GetOp retrieves an op of the operation from the collection by id
	GetOp(string) (operations.Op, error)

	// Walk over a collection of operations
	Walk(func(operations.Op) error) error
}

// API defines a operations API
type API struct {
	api.DefaultService
	name   string
	logger log.Logger
}

// NewAPI creates a API with sane defaults
func NewAPI(name string, options ...Option) *API {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &API{
		name:   name,
		logger: opts.logger,
	}
}

// Name returns the API name
func (a *API) Name() string {
	return a.name
}

// Get defines a service for calling "GET" method and returns a response.
func (a *API) Get(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var result map[string]Op
	if err := d.Operations().Walk(func(op operations.Op) error {
		result[op.ID] = Op{
			ID:         op.ID,
			URL:        fmt.Sprintf("/%s%s", d.Version(), op.URL),
			Class:      op.Class,
			Status:     op.Status,
			StatusCode: op.StatusCode.Raw(),
			Err:        op.Err,
		}
		return nil
	}); err != nil {
		return api.SmartError(err)
	}

	// verify if we're clustered or not
	clustered, err := cluster.Enabled(d.Node())
	if err != nil {
		return api.InternalError(err)
	}
	if !clustered {
		return api.SyncResponse(true, result)
	}

	var nodes []string
	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		nodes, err = tx.OperationNodes()
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		return api.SmartError(err)
	}

	// Get local addres
	localAddress, err := node.HTTPSAddress(d.Node(), d.NodeConfigSchema())
	if err != nil {
		return api.InternalError(err)
	}

	cert := d.Endpoints().NetworkCert()
	for _, node := range nodes {
		if node == localAddress {
			continue
		}

		client, err := getClient(node, cert, a.logger)
		if err != nil {
			return api.SmartError(err)
		}

		resp, _, err := client.Query("GET", "/1.0/operations", nil, "")
		if err != nil {
			return api.SmartError(err)
		} else if resp.StatusCode != 200 {
			return api.SmartError(errors.Errorf("invalid status code %v", resp.StatusCode))
		}

		var ops map[string]Op
		if err := json.Unmarshal(resp.Metadata, &ops); err != nil {
			return api.InternalError(err)
		}

		for k, v := range ops {
			result[k] = v
		}
	}

	return api.SyncResponse(true, result)
}

func getClient(address string, certInfo *cert.Info, logger log.Logger) (*client.Client, error) {
	return client.New(
		fmt.Sprintf("https://%s", address),
		client.WithLogger(log.WithPrefix(logger, "component", "client")),
		client.WithTLSServerCert(string(certInfo.PublicKey())),
		client.WithTLSClientCert(string(certInfo.PublicKey())),
		client.WithTLSClientKey(string(certInfo.PrivateKey())),
	)
}

package operations

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// IdentityAPI defines a operations/{id} API
type IdentityAPI struct {
	api.DefaultService
	name   string
	logger log.Logger
}

// NewIdentityAPI creates a API with sane defaults
func NewIdentityAPI(name string, options ...Option) *IdentityAPI {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &IdentityAPI{
		name:   name,
		logger: opts.logger,
	}
}

// Name returns the IdentityAPI name
func (a *IdentityAPI) Name() string {
	return a.name
}

// Get defines a service for calling "GET" method and returns a response.
func (a *IdentityAPI) Get(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	id, ok := mux.Vars(req)["id"]
	if !ok {
		return api.BadRequest(errors.Errorf("expected id"))
	}

	// First check locally on the node
	op, err := d.Operations().GetOpByPartialID(id)
	if err == nil {
		return api.SyncResponse(true, Op{
			ID:         op.ID,
			URL:        op.URL,
			Class:      op.Class,
			Status:     op.Status,
			StatusCode: op.StatusCode.Raw(),
			Err:        op.Err,
		})
	}

	// Then check other nodes
	var address string
	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		operation, err := tx.OperationByUUID(id)
		if err != nil {
			return errors.WithStack(err)
		}

		address = operation.NodeAddress
		return nil
	}); err != nil {
		return api.SmartError(err)
	}

	cert := d.Endpoints().NetworkCert()
	client, err := getClient(address, cert, a.logger)
	if err != nil {
		return api.SmartError(err)
	}

	resp, _, err := client.Query("GET", fmt.Sprintf("/%s/operations/%s", d.Version(), id), nil, "")
	if err != nil {
		return api.SmartError(err)
	} else if resp.StatusCode != 200 {
		return api.SmartError(errors.Errorf("invalid status code %v", resp.StatusCode))
	}

	var result Op
	if err := json.Unmarshal(resp.Metadata, &result); err != nil {
		return api.InternalError(err)
	}

	return api.SyncResponse(true, result)
}

// Delete defines a service for calling "DELETE" method and returns a response.
func (a *IdentityAPI) Delete(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	id, ok := mux.Vars(req)["id"]
	if !ok {
		return api.BadRequest(errors.Errorf("expected id"))
	}

	// First check locally on the node
	if op, err := d.Operations().GetOpByPartialID(id); err == nil {
		if err := d.Operations().DeleteOp(op.ID); err != nil {
			return api.BadRequest(err)
		}
		return api.EmptySyncResponse()
	}

	// Then check other nodes
	var address string
	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		operation, err := tx.OperationByUUID(id)
		if err != nil {
			return errors.WithStack(err)
		}

		address = operation.NodeAddress
		return nil
	}); err != nil {
		return api.SmartError(err)
	}

	cert := d.Endpoints().NetworkCert()
	client, err := getClient(address, cert, a.logger)
	if err != nil {
		return api.SmartError(err)
	}

	resp, _, err := client.Query("DELETE", fmt.Sprintf("/%s/operations/%s", d.Version(), id), nil, "")
	if err != nil {
		return api.SmartError(err)
	} else if resp.StatusCode != 200 {
		return api.SmartError(errors.Errorf("invalid status code %v", resp.StatusCode))
	}

	return api.EmptySyncResponse()
}

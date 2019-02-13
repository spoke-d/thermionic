package schedules

import (
	"context"
	"fmt"
	"net/http"

	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// IdentityAPI defines a schedules/{id} API
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
	tsk, err := d.Schedules().GetTsk(id)
	if err != nil {
		return api.SmartError(err)
	}

	return api.SyncResponse(true, Task{
		ID:         tsk.ID,
		URL:        fmt.Sprintf("/%s%s", d.Version(), tsk.URL),
		Status:     tsk.Status,
		StatusCode: tsk.StatusCode.Raw(),
		Result:     tsk.Result,
		Err:        tsk.Err,
	})
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

	if err := d.Schedules().Remove(id); err != nil {
		return api.BadRequest(err)
	}
	return api.EmptySyncResponse()
}

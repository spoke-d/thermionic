package shutdown

import (
	"context"
	"net/http"

	"github.com/spoke-d/thermionic/pkg/api"
)

// API defines a query API
type API struct {
	api.DefaultService
	name string
}

// NewAPI creates a API with sane defaults
func NewAPI(name string) *API {
	return &API{
		name: name,
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

	d.UnsafeShutdown()

	return api.EmptySyncResponse()
}

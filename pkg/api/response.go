package api

import (
	"database/sql"
	"net/http"
	"os"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/go-kit/kit/log"
	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/etag"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/pkg/client"
)

// Response defines a return value from a http request. The response then can
// be rendered.
type Response interface {

	// Render the response with a response writer.
	Render(http.ResponseWriter) error
}

// SyncResponse defines a response that is synchronous
func SyncResponse(success bool, metadata interface{}) Response {
	return &syncResponse{
		success:  success,
		metadata: metadata,
	}
}

// SyncResponseRedirect defines a successful response that will aways perform a
// permanent redirect.
func SyncResponseRedirect(address string) Response {
	return &syncResponse{
		success:  true,
		location: address,
		code:     http.StatusPermanentRedirect,
	}
}

// SyncResponseETag defines a response that can add ETag as additional
// information
func SyncResponseETag(success bool, metadata interface{}, eTag interface{}) Response {
	return &syncResponse{
		success:  success,
		metadata: metadata,
		eTag:     eTag,
	}
}

// EmptySyncResponse defines an empty successful response
func EmptySyncResponse() Response {
	return &syncResponse{
		success:  true,
		metadata: make(map[string]interface{}),
	}
}

// Sync response
type syncResponse struct {
	success  bool
	eTag     interface{}
	metadata interface{}
	location string
	code     int
	headers  map[string]string
	logger   log.Logger
}

// Render will consume a http.ResponseWriter and return an error in a vistor
// pattern scenario.
func (r *syncResponse) Render(w http.ResponseWriter) error {
	// Set an appropriate ETag header
	if r.eTag != nil {
		if eTag, err := etag.Hash(r.eTag); err == nil {
			w.Header().Set("ETag", eTag)
		}
	}

	status := http.StatusOK
	if !r.success {
		status = http.StatusBadRequest
	}

	if r.headers != nil {
		for h, v := range r.headers {
			w.Header().Set(h, v)
		}
	}

	if r.location != "" {
		w.Header().Set("Location", r.location)
		if r.code == 0 {
			w.WriteHeader(201)
		} else {
			w.WriteHeader(r.code)
		}
	}

	return json.Write(w, client.ResponseRaw{
		Type:       client.SyncResponse,
		Status:     http.StatusText(status),
		StatusCode: status,
		Metadata:   r.metadata,
	}, false, r.logger)
}

// Error response
type errorResponse struct {
	code   int
	msg    string
	logger log.Logger
}

func (r *errorResponse) String() string {
	return r.msg
}

// Render will consume a http.ResponseWriter and return an error in a vistor
// pattern scenario.
func (r *errorResponse) Render(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	w.WriteHeader(r.code)

	return json.Write(w, map[string]interface{}{
		"type":       client.ErrorResponse,
		"error":      r.msg,
		"error_code": r.code,
	}, false, r.logger)
}

// NotImplemented takes an error and returns a Response of not implemented.
func NotImplemented(err error) Response {
	message := "not implemented"
	if err != nil {
		message = err.Error()
	}
	return &errorResponse{
		code: http.StatusNotImplemented,
		msg:  message,
	}
}

// NotFound takes an error and returns a Response of not found.
func NotFound(err error) Response {
	message := "not found"
	if err != nil {
		message = err.Error()
	}
	return &errorResponse{
		code: http.StatusNotFound,
		msg:  message,
	}
}

// Forbidden takes an error and returns a Response of forbidden error.
func Forbidden(err error) Response {
	message := "not authorized"
	if err != nil {
		message = err.Error()
	}
	return &errorResponse{
		code: http.StatusForbidden,
		msg:  message,
	}
}

// Conflict takes an error and returns a Response of conflict error.
func Conflict(err error) Response {
	message := "already exists"
	if err != nil {
		message = err.Error()
	}
	return &errorResponse{
		code: http.StatusConflict,
		msg:  message,
	}
}

// Unavailable takes an error and returns a Response of unavailable error.
func Unavailable(err error) Response {
	message := "unavailable"
	if err != nil {
		message = err.Error()
	}
	return &errorResponse{
		code: http.StatusServiceUnavailable,
		msg:  message,
	}
}

// BadRequest takes an error and returns a Response of badrequest error.
func BadRequest(err error) Response {
	return &errorResponse{
		code: http.StatusBadRequest,
		msg:  err.Error(),
	}
}

// InternalError takes an error and returns a Response of internal server error.
func InternalError(err error) Response {
	return &errorResponse{
		code: http.StatusInternalServerError,
		msg:  err.Error(),
	}
}

// PreconditionFailed takes an error and returns a Response of precondition
// failed error.
func PreconditionFailed(err error) Response {
	return &errorResponse{
		code: http.StatusPreconditionFailed,
		msg:  err.Error(),
	}
}

//SmartError returns the right error message based on err.
func SmartError(err error) Response {
	switch errors.Cause(err) {
	case nil:
		return EmptySyncResponse()
	case os.ErrNotExist:
		return NotFound(nil)
	case sql.ErrNoRows:
		return NotFound(nil)
	case db.ErrNoSuchObject:
		return NotFound(nil)
	case os.ErrPermission:
		return Forbidden(nil)
	case db.ErrAlreadyDefined:
		return Conflict(nil)
	case sqlite3.ErrConstraintUnique:
		return Conflict(nil)
	case dqlite.ErrNoAvailableLeader:
		return Unavailable(err)
	default:
		return InternalError(err)
	}
}

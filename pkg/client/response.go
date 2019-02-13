package client

import "encoding/json"

// ResponseType represents a valid response type
type ResponseType string

// Response types
const (
	SyncResponse  ResponseType = "sync"
	AsyncResponse ResponseType = "async"
	ErrorResponse ResponseType = "error"
)

// Response represents a operation
type Response struct {
	Type ResponseType `json:"type" yaml:"type"`

	// Valid only for Sync responses
	Status     string `json:"status" yaml:"status"`
	StatusCode int    `json:"status_code" yaml:"status_code"`

	// Valid only for Async responses
	Operation string `json:"operation" yaml:"operation"`

	// Valid only for Error responses
	Code  int    `json:"error_code" yaml:"error_code"`
	Error string `json:"error" yaml:"error"`

	// Valid for Sync and Error responses
	Metadata json.RawMessage `json:"metadata" yaml:"metadata"`
}

// ResponseRaw represents a operation
type ResponseRaw struct {
	Type ResponseType `json:"type" yaml:"type"`

	// Valid only for Sync responses
	Status     string `json:"status" yaml:"status"`
	StatusCode int    `json:"status_code" yaml:"status_code"`

	// Valid only for Async responses
	Operation string `json:"operation" yaml:"operation"`

	// Valid only for Error responses
	Code  int    `json:"error_code" yaml:"error_code"`
	Error string `json:"error" yaml:"error"`

	// Valid for Sync and Error responses
	Metadata interface{} `json:"metadata" yaml:"metadata"`
}

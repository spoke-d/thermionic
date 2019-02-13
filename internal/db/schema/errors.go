package schema

import "github.com/pkg/errors"

// ErrGracefulAbort is a special error that can be returned by a Check function
// to force Schema.Ensure to abort gracefully.
//
// Every change performed so by the Check will be committed, although
// ErrGracefulAbort will be returned.
var ErrGracefulAbort = errors.Errorf("schema check gracefully aborted")

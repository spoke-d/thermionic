package raft

import (
	rafthttp "github.com/CanonicalLtd/raft-http"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
)

// NewDialer creates a rafthttp.Dial function that connects over TLS using the
// given cluster (and optionally CA) certificate both as client and remote
// certificate.
func NewDialer(info *cert.Info) (rafthttp.Dial, error) {
	config, err := cert.TLSClientConfig(info)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	dial := rafthttp.NewDialTLS(config)
	return dial, nil
}

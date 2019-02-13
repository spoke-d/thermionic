package discovery

import (
	"github.com/spoke-d/thermionic/internal/discovery/members"
	"github.com/pkg/errors"
)

const (
	// PeerTypeStore serves the store API
	PeerTypeStore members.PeerType = "store"
)

// ParsePeerType parses a potential peer type and errors out if it's not a known
// valid type.
func ParsePeerType(t string) (members.PeerType, error) {
	switch t {
	case "store":
		return members.PeerType(t), nil
	default:
		return "", errors.Errorf("unknown peer type (%s)", t)
	}
}

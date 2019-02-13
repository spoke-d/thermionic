package members

import (
	"github.com/pkg/errors"
)

// PeerType describes the type of peer with in the cluster.
type PeerType string

func (p PeerType) String() string {
	return string(p)
}

// PeerInfo describes what each peer is, along with the addr and port of each
type PeerInfo struct {
	Name          string   `json:"name"`
	Type          PeerType `json:"type"`
	DaemonAddress string   `json:"daemon_address"`
	DaemonNonce   string   `json:"daemon_nonce"`
}

// encodeTagPeerInfo encodes the peer information for the node tags.
func encodePeerInfoTag(info PeerInfo) map[string]string {
	return map[string]string{
		"name":           info.Name,
		"type":           string(info.Type),
		"daemon_address": info.DaemonAddress,
		"daemon_nonce":   info.DaemonNonce,
	}
}

// decodePeerInfoTag gets the peer information from the node tags.
func decodePeerInfoTag(m map[string]string) (info PeerInfo, err error) {
	name, ok := m["name"]
	if !ok {
		err = errors.Errorf("missing name")
		return
	}
	info.Name = name

	peerType, ok := m["type"]
	if !ok {
		err = errors.Errorf("missing api_addr")
		return
	}
	info.Type = PeerType(peerType)

	if info.DaemonAddress, ok = m["daemon_address"]; !ok {
		err = errors.Errorf("missing daemon_address")
		return
	}

	if info.DaemonNonce, ok = m["daemon_nonce"]; !ok {
		err = errors.Errorf("missing daemon_nonce")
		return
	}
	return
}

package db

import (
	"time"

	"github.com/spoke-d/thermionic/internal/clock"
)

// ServiceNodeInfo holds information about a single instance in a cluster.
type ServiceNodeInfo struct {
	ID            int64     // Stable node identifier
	Name          string    // User-assigned name of the node
	Address       string    // Network address of the node
	DaemonAddress string    // Daemon address that's associated with the service
	DaemonNonce   string    // Daemon nonce for secure communication with the dameon
	Heartbeat     time.Time // Timestamp of the last heartbeat
}

// IsOffline returns true if the last successful heartbeat time of the node is
// older than the given threshold.
func (n ServiceNodeInfo) IsOffline(clock clock.Clock, threshold time.Duration) bool {
	return nodeIsOffline(clock, threshold, n.Heartbeat)
}

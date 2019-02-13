package db

import (
	"time"

	"github.com/spoke-d/thermionic/internal/clock"
)

// NodeInfo holds information about a single instance in a cluster.
type NodeInfo struct {
	ID            int64     // Stable node identifier
	Name          string    // User-assigned name of the node
	Address       string    // Network address of the node
	Description   string    // Node description (optional)
	Schema        int       // Schema version of the code running the node
	APIExtensions int       // Number of API extensions of the code running on the node
	Heartbeat     time.Time // Timestamp of the last heartbeat
}

// IsOffline returns true if the last successful heartbeat time of the node is
// older than the given threshold.
func (n NodeInfo) IsOffline(clock clock.Clock, threshold time.Duration) bool {
	return nodeIsOffline(clock, threshold, n.Heartbeat)
}

// Version returns the node's version, composed by its schema level and
// number of extensions.
func (n NodeInfo) Version() [2]int {
	return [2]int{
		n.Schema,
		n.APIExtensions,
	}
}

func nodeIsOffline(clock clock.Clock, threshold time.Duration, heartbeat time.Time) bool {
	return heartbeat.Before(clock.Now().Add(-threshold))
}

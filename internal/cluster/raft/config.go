package raft

import (
	"math"
	"time"

	"github.com/hashicorp/raft"
)

// NewConfig will create a base raft configuration tweaked for a network with
// the given latency measure.
func NewConfig(latency float64) *raft.Config {
	config := raft.DefaultConfig()
	scale := func(duration *time.Duration) {
		*duration = time.Duration((math.Ceil(float64(*duration) * latency)))
	}
	durations := []*time.Duration{
		&config.HeartbeatTimeout,
		&config.ElectionTimeout,
		&config.CommitTimeout,
		&config.LeaderLeaseTimeout,
	}
	for _, duration := range durations {
		scale(duration)
	}

	config.SnapshotThreshold = 1024
	config.TrailingLogs = 512

	return config
}

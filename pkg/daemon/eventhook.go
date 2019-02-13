package daemon

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// NewEventHook forwards to the local events dispatcher an event received
// from another node.
func NewEventHook(broadcaster EventBroadcaster, logger log.Logger) func(int64, interface{}) {
	return func(id int64, data interface{}) {
		event := data.(map[string]interface{})
		event["node"] = id

		if err := broadcaster.Dispatch(event); err != nil {
			level.Warn(logger).Log(
				"msg", "Failed to forward event from node",
				"id", id,
				"err", err,
			)
		}
	}
}

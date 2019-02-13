package events

import (
	"encoding/json"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// Actor defines an broker between event messages and nodes
type Actor interface {

	// ID returns the unique ID for the actor
	ID() string

	// Types returns the underlying types the actor subscribes to
	Types() []string

	// Write pushes information to the actor
	Write([]byte) error

	// Close the actor
	Close()

	// NoForward decides if messages should be forwarded to the actor
	NoForward() bool

	// Done returns if the actor is done messaging.
	Done() bool
}

// ActorGroup holds a group of actors
type ActorGroup interface {

	// Add an actor to a group
	Add(a Actor)

	// Prune removes any done actors
	Prune() bool

	// Walk over the actors with in the group one by one (order is not
	// guaranteed).
	Walk(func(Actor) error) error
}

// EventBroadcaster allows messages to be dispatched to other actors.
type EventBroadcaster struct {
	actorGroup ActorGroup
	logger     log.Logger
}

// NewEventBroadcaster creates a new EventBroadcaster
func NewEventBroadcaster(actorGroup ActorGroup, logger log.Logger) *EventBroadcaster {
	return &EventBroadcaster{
		actorGroup: actorGroup,
		logger:     logger,
	}
}

// Dispatch an event to other awaiting actors.
func (e *EventBroadcaster) Dispatch(event map[string]interface{}) error {
	body, err := json.Marshal(event)
	if err != nil {
		return errors.WithStack(err)
	}

	_, isForward := event["node"]

	// remove any actors that might have already been done, then clean the
	// actors up
	e.actorGroup.Prune()
	return e.actorGroup.Walk(func(actor Actor) error {
		if isForward && actor.NoForward() {
			return nil
		}

		eventType, ok := event["type"].(string)
		if !ok {
			return nil
		}
		if !contains(actor.Types(), eventType) {
			return nil
		}

		go func(actor Actor, body []byte) {
			// Check that the actor still exists
			if actor == nil {
				return
			}

			if err := actor.Write(body); err != nil {
				actor.Close()
				level.Debug(e.logger).Log("msg", "Disconnected actor", "id", actor.ID())
			}
		}(actor, body)

		return nil
	})
}

func contains(a []string, b string) bool {
	for _, v := range a {
		if v == b {
			return true
		}
	}
	return false
}

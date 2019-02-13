package actors

import (
	"sync"

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

// Group holds a group of actors
type Group struct {
	actors map[string]Actor
	mutex  sync.Mutex
}

// NewGroup creates an Group with sane defaults
func NewGroup() *Group {
	return &Group{
		actors: make(map[string]Actor),
	}
}

// Add an actor to a group
func (g *Group) Add(a Actor) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.actors[a.ID()] = a
}

// Remove an actor from the group
func (g *Group) Remove(a Actor) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	delete(g.actors, a.ID())
}

// Prune removes any done actors
func (g *Group) Prune() (cleaned bool) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	for _, actor := range g.actors {
		if actor.Done() {
			delete(g.actors, actor.ID())
			cleaned = true
		}
	}
	return
}

// Walk over the actors with in the group one by one (order is not
// guaranteed).
func (g *Group) Walk(fn func(Actor) error) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	for _, actor := range g.actors {
		if err := fn(actor); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

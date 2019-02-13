package events

import (
	"sync"

	"github.com/pkg/errors"
)

// EventTarget struct is returned to the caller of AddHandler and used in
// RemoveHandler
type EventTarget struct {
	function func(interface{})
	types    []string
}

// EventListener struct is used to interact with a event stream
type EventListener struct {
	events       *Events
	chActive     chan bool
	disconnected bool
	err          error

	targets     []*EventTarget
	targetsLock sync.Mutex
}

// AddHandler adds a function to be called whenever an event is received
func (e *EventListener) AddHandler(types []string, function func(interface{})) *EventTarget {
	// Handle locking
	e.targetsLock.Lock()
	defer e.targetsLock.Unlock()

	// Create a new target
	target := EventTarget{
		function: function,
		types:    types,
	}

	// And add it to the targets
	e.targets = append(e.targets, &target)

	return &target
}

// RemoveHandler removes a function to be called whenever an event is received
func (e *EventListener) RemoveHandler(target *EventTarget) error {
	// Handle locking
	e.targetsLock.Lock()
	defer e.targetsLock.Unlock()

	// Locate and remove the function from the list
	for i, entry := range e.targets {
		if entry == target {
			copy(e.targets[i:], e.targets[i+1:])
			e.targets[len(e.targets)-1] = nil
			e.targets = e.targets[:len(e.targets)-1]
			return nil
		}
	}

	return errors.Errorf("function and event type not found")
}

// Disconnect must be used once done listening for events
func (e *EventListener) Disconnect() {
	if e.disconnected {
		return
	}

	// Handle locking
	e.events.eventListenersLock.Lock()
	defer e.events.eventListenersLock.Unlock()

	// Locate and remove it from the global list
	for i, listener := range e.events.eventListeners {
		if listener == e {
			copy(e.events.eventListeners[i:], e.events.eventListeners[i+1:])
			e.events.eventListeners[len(e.events.eventListeners)-1] = nil
			e.events.eventListeners = e.events.eventListeners[:len(e.events.eventListeners)-1]
			break
		}
	}

	// Turn off the handler
	e.err = nil
	e.disconnected = true
	close(e.chActive)
}

// Wait hangs until the server disconnects the connection or Disconnect() is called
func (e *EventListener) Wait() error {
	<-e.chActive
	return errors.WithStack(e.err)
}

// IsActive returns true if this listener is still connected, false otherwise.
func (e *EventListener) IsActive() bool {
	select {
	case <-e.chActive:
		// If the chActive channel is closed we got disconnected
		return false
	default:
		return true
	}
}

// ActiveChan returns the underlying channel
func (e *EventListener) ActiveChan() chan bool {
	return e.chActive
}

// Err return the potentially underlying error
func (e *EventListener) Err() error {
	return e.err
}

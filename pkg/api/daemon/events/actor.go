package events

import (
	"sync"

	"github.com/gorilla/websocket"
)

// Conn represents a connection message
type Conn interface {
	WriteMessage(int, []byte) error
	Close() error
}

type actor struct {
	mutex        sync.Mutex
	connection   Conn
	messageTypes []string
	active       chan bool
	id           string
	done         bool

	// If true, this listener won't get events forwarded from other
	// nodes. It only used by listeners created internally by nodes
	// connecting to other nodes to get their local events only.
	noForward bool
}

// ID returns the unique ID for the actor
func (a *actor) ID() string {
	return a.id
}

// Types returns the underlying types the actor subscribes to
func (a *actor) Types() []string {
	return a.messageTypes
}

// Write pushes information to the actor
func (a *actor) Write(body []byte) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	return a.connection.WriteMessage(websocket.TextMessage, body)
}

// Close the actor
func (a *actor) Close() {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.connection.Close()
	a.active <- false
	a.done = true
}

// NoForward decides if messages should be forwarded to the actor
func (a *actor) NoForward() bool {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	return a.noForward
}

// Done returns if the actor is done messaging.
func (a *actor) Done() bool {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	return a.done
}

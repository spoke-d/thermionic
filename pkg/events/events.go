package events

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clock"
)

// Client represents a way to interact with the server API
type Client interface {

	// Websocket allows directly connection to API websockets
	Websocket(path string) (*websocket.Conn, error)
}

// Events represents the API for the Events
type Events struct {
	client Client
	clock  clock.Clock

	eventListeners     []*EventListener
	eventListenersLock sync.Mutex
}

// NewEvents creates an Events API with sane defaults
func NewEvents(client Client, options ...Option) *Events {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Events{
		client: client,
		clock:  opts.clock,
	}
}

// GetEvents connects to monitoring interface
func (e *Events) GetEvents() (*EventListener, error) {
	// Prevent anything else from interacting with the listeners
	e.eventListenersLock.Lock()
	defer e.eventListenersLock.Unlock()

	// Setup a new listener
	listener := &EventListener{
		events:   e,
		chActive: make(chan bool),
	}

	if e.eventListeners != nil {
		// There is an existing Go routine setup, so just add another target
		e.eventListeners = append(e.eventListeners, listener)
		return listener, nil
	}

	// Initialize the list if needed
	e.eventListeners = make([]*EventListener, 0)

	// Setup a new connection
	conn, err := e.client.Websocket("/events")
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Add the listener
	e.eventListeners = append(e.eventListeners, listener)

	// Spawn a watcher that will close the websocket connection after all
	// listeners are gone.
	stopCh := make(chan struct{}, 0)
	go func() {
		for {
			select {
			case <-e.clock.After(time.Minute):
			case <-stopCh:
				break
			}

			e.eventListenersLock.Lock()
			if len(e.eventListeners) == 0 {
				// We don't need the connection anymore, disconnect
				conn.Close()

				e.eventListeners = nil
				e.eventListenersLock.Unlock()
				return
			}
			e.eventListenersLock.Unlock()
		}
	}()

	// And spawn the listener
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				// Prevent anything else from interacting with the listeners
				e.eventListenersLock.Lock()
				defer e.eventListenersLock.Unlock()

				// Tell all the current listeners about the failure
				for _, listener := range e.eventListeners {
					listener.err = err
					listener.disconnected = true
					close(listener.chActive)
				}

				// And remove them all from the list
				e.eventListeners = nil

				conn.Close()
				close(stopCh)

				return
			}

			message, messageType, err := parseMessage(data)
			if err != nil {
				continue
			}

			// Send the message to all handlers
			e.eventListenersLock.Lock()
			for _, listener := range e.eventListeners {
				listener.targetsLock.Lock()
				for _, target := range listener.targets {
					if target.types != nil && !contains(target.types, messageType) {
						continue
					}

					go target.function(message)
				}
				listener.targetsLock.Unlock()
			}
			e.eventListenersLock.Unlock()
		}
	}()

	return listener, nil
}

func parseMessage(data []byte) (map[string]interface{}, string, error) {
	// Attempt to unpack the message
	message := make(map[string]interface{})
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, "", errors.WithStack(err)
	}

	// Extract the message type
	_, ok := message["type"]
	if !ok {
		return nil, "", errors.New("invalid message type")
	}
	messageType := message["type"].(string)
	return message, messageType, nil
}

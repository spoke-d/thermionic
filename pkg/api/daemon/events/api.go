package events

import (
	"context"
	"net/http"
	"strings"

	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/spoke-d/thermionic/pkg/events"
	"github.com/gorilla/websocket"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
)

// Daemon can respond to requests from a shared client.
type Daemon interface {

	// ActorGroup returns the actor group associated with the daemon
	ActorGroup() events.ActorGroup
}

// API defines a events API
type API struct {
	api.DefaultService
	name       string
	wsUpgrader websocket.Upgrader
}

// NewAPI creates a API with sane defaults
func NewAPI(name string) *API {
	return &API{
		name: name,
		wsUpgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

// Name returns the API name
func (a *API) Name() string {
	return a.name
}

// Get defines a service for calling "GET" method and returns a response.
func (a *API) Get(ctx context.Context, req *http.Request) api.Response {
	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	return &service{
		actorGroup: makeActorGroupShim(d.ActorGroup()),
		req:        req,
		wsUpgrader: a.wsUpgrader,
	}
}

type service struct {
	actorGroup events.ActorGroup
	req        *http.Request
	wsUpgrader websocket.Upgrader
}

func (s *service) Render(w http.ResponseWriter) error {
	typeStr := s.req.FormValue("type")
	if typeStr == "" {
		typeStr = "logging,operation,lifecycle"
	}

	c, err := s.wsUpgrader.Upgrade(w, s.req, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	a := &actor{
		id: uuid.NewRandom().String(),
		connection: connShim{
			conn: c,
		},
		active:       make(chan bool, 1),
		messageTypes: strings.Split(typeStr, ","),
		noForward:    api.IsClusterNotification(s.req),
	}
	s.actorGroup.Add(a)

	// wait until we're ready to finish rendering.
	<-a.active

	return nil
}

type connShim struct {
	conn *websocket.Conn
}

func (c connShim) WriteMessage(i int, b []byte) error {
	return c.conn.WriteMessage(i, b)
}

func (c connShim) Close() error {
	return c.conn.Close()
}

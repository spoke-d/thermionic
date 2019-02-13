package events

import (
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/spoke-d/thermionic/pkg/events"
)

type actorGroupShim struct {
	actorGroup api.ActorGroup
}

func makeActorGroupShim(actorGroup api.ActorGroup) actorGroupShim {
	return actorGroupShim{
		actorGroup: actorGroup,
	}
}

func (s actorGroupShim) Add(a events.Actor) {
	s.actorGroup.Add(a)
}

func (s actorGroupShim) Prune() bool {
	return s.actorGroup.Prune()
}

func (s actorGroupShim) Walk(fn func(events.Actor) error) error {
	return s.actorGroup.Walk(func(a api.Actor) error {
		return fn(a)
	})
}

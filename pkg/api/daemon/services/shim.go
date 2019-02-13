package services

import (
	"github.com/spoke-d/thermionic/internal/cluster/services"
	"github.com/spoke-d/thermionic/pkg/api"
)

type servicesStateShim struct {
	state api.State
}

func makeServicesStateShim(state api.State) servicesStateShim {
	return servicesStateShim{
		state: state,
	}
}

func (s servicesStateShim) Node() services.Node {
	return s.state.Node()
}

func (s servicesStateShim) Cluster() services.Cluster {
	return s.state.Cluster()
}

func (s servicesStateShim) OS() services.OS {
	return s.state.OS()
}

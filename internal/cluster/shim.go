package cluster

import (
	"context"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/spoke-d/thermionic/internal/db/cluster"
)

type serverStoreShim struct {
	store dqlite.ServerStore
}

func makeServerStore(store dqlite.ServerStore) serverStoreShim {
	return serverStoreShim{
		store: store,
	}
}

// Get return the list of known servers.
func (s serverStoreShim) Get(ctx context.Context) ([]cluster.ServerInfo, error) {
	info, err := s.store.Get(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]cluster.ServerInfo, len(info))
	for k, v := range info {
		res[k] = cluster.ServerInfo{
			ID:      v.ID,
			Address: v.Address,
		}
	}
	return res, nil
}

// Set updates the list of known cluster servers.
func (s serverStoreShim) Set(ctx context.Context, info []cluster.ServerInfo) error {
	res := make([]dqlite.ServerInfo, len(info))
	for k, v := range info {
		res[k] = dqlite.ServerInfo{
			ID:      v.ID,
			Address: v.Address,
		}
	}
	return s.store.Set(ctx, res)
}

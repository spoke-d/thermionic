package raft

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/lru"
	"github.com/spoke-d/thermionic/internal/task"
)

// Interval represents the number of seconds to wait between to gc
// rounds.
const Interval = 4

// AddressProvider is a address provider that looks up server addresses in the
// raft_nodes table.
type AddressProvider struct {
	db Node
}

// NewAddressProvider creates a AddressProvider with sane defaults
func NewAddressProvider(db Node) *AddressProvider {
	return &AddressProvider{
		db: db,
	}
}

// ServerAddr gets the raft ServerAddress from the raft nodes, depending on
// the id supplied
func (p *AddressProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	databaseID, err := strconv.Atoi(string(id))
	if err != nil {
		return "", errors.Wrap(err, "non-numeric server ID")
	}

	var address string
	if err := p.db.Transaction(func(tx *db.NodeTx) error {
		var err error
		address, err = tx.RaftNodeAddress(int64(databaseID))
		return errors.WithStack(err)
	}); err != nil {
		return "", errors.WithStack(err)
	}
	return raft.ServerAddress(address), nil
}

// CacheAddressProvider provides an implementation that attempts to cache
// addresses for use for server address function, without hitting the database
// for every lookup.
type CacheAddressProvider struct {
	provider *AddressProvider
	cache    *lru.LRU
	mutex    sync.RWMutex
}

// NewCacheAddressProvider creates a AddressProvider with sane defaults
func NewCacheAddressProvider(db Node) *CacheAddressProvider {
	return &CacheAddressProvider{
		provider: NewAddressProvider(db),
	}
}

// ServerAddr gets the raft ServerAddress from the raft nodes, depending on
// the id supplied
func (p *CacheAddressProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	databaseID, err := strconv.Atoi(string(id))
	if err != nil {
		return "", errors.Wrap(err, "non-numeric server ID")
	}

	p.mutex.RLock()
	address, ok := p.cache.Get(databaseID)
	p.mutex.RUnlock()
	if ok && address != "" {
		return raft.ServerAddress(address), nil
	}

	addr, err := p.provider.ServerAddr(id)
	if err != nil {
		return "", errors.WithStack(err)
	}

	p.mutex.Lock()
	p.cache.Add(databaseID, string(addr))
	p.mutex.Unlock()

	return raft.ServerAddress(addr), nil
}

// Run returns a task function that performs cleanup of raft node addresses
func (p *CacheAddressProvider) Run() (task.Func, task.Schedule) {
	addressProviderWrapper := func(ctx context.Context) {
		ch := make(chan struct{})
		go func() {
			p.run(ctx)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}
	schedule := task.Every(time.Duration(Interval) * time.Second)
	return addressProviderWrapper, schedule
}

func (p *CacheAddressProvider) run(ctx context.Context) {
	p.mutex.Lock()
	p.cache.Purge()
	p.mutex.Unlock()
}

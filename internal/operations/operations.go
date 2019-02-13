package operations

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/task"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// Cluster mediates access to data stored in the cluster dqlite database.
type Cluster interface {
	database.DBAccessor
	db.ClusterOpener
	db.ClusterTransactioner
	db.ClusterExclusiveLocker

	// NodeID sets the the node NodeID associated with this cluster instance. It's used for
	// backward-compatibility of all db-related APIs that were written before
	// clustering and don't accept a node NodeID, so in those cases we automatically
	// use this value as implicit node NodeID.
	NodeID(int64)

	// Close the database facade.
	Close() error
}

// Interval represents the number of seconds to wait between to operational
// GC rounds.
const Interval = 60

// Operations represents a operational collection of things that want to be
// run
type Operations struct {
	cluster    Cluster
	operations map[string]*Operation
	mutex      sync.Mutex
	logger     log.Logger
}

// New creates a series of operations to be worked on
func New(cluster Cluster, options ...Option) *Operations {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Operations{
		cluster:    cluster,
		operations: make(map[string]*Operation),
		logger:     opts.logger,
	}
}

// Add an operation to the collection
func (o *Operations) Add(op *Operation) error {
	if err := o.cluster.Transaction(func(tx *db.ClusterTx) error {
		_, err := tx.OperationAdd(op.id, db.OperationType(Task.String()))
		return errors.WithStack(err)
	}); err != nil {
		return errors.Wrapf(err, "failed to add operation %s to database", op.id)
	}

	o.mutex.Lock()
	o.operations[op.id] = op
	o.mutex.Unlock()

	return nil
}

// Remove an operation from the collection
func (o *Operations) Remove(id string) error {
	o.mutex.Lock()
	op, ok := o.operations[id]
	o.mutex.Unlock()
	if !ok {
		return errors.Errorf("operation not found for %q", id)
	}

	_, err := op.Cancel()
	if err != nil {
		return errors.Wrap(err, "failed to remove operation")
	}

	if err := o.cluster.Transaction(func(tx *db.ClusterTx) error {
		err := tx.OperationRemove(id)
		return errors.WithStack(err)
	}); err != nil {
		return errors.Wrapf(err, "failed to remove operation %s to database", id)
	}

	o.mutex.Lock()
	delete(o.operations, id)
	o.mutex.Unlock()

	return nil
}

// GetOpByID retrieves an op of the operation from the collection by id
func (o *Operations) GetOpByID(id string) (Op, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if op, ok := o.operations[id]; ok {
		return op.Render(), nil
	}
	return Op{}, errors.Errorf("operation %q not found", id)
}

// GetOpByPartialID retrieves an op of the operation from the collection by a
// partial id. As long as the prefix matches an id then it will return that
// operation. If the id matches multiple ids then it will return an ambiguous
// error.
func (o *Operations) GetOpByPartialID(id string) (Op, error) {
	if id == "" {
		return Op{}, errors.Errorf("expected id")
	}

	op, err := o.GetOpByID(id)
	if err == nil {
		return op, nil
	}

	o.mutex.Lock()
	var results []Op
	for k, v := range o.operations {
		if strings.HasPrefix(k, id) {
			results = append(results, v.Render())
		}
	}
	o.mutex.Unlock()

	if num := len(results); num == 0 {
		return Op{}, errors.Errorf("operation %q not found", id)
	} else if num > 1 {
		return Op{}, errors.Errorf("ambiguous operation %q, too many matches", id)
	}
	return results[1], nil
}

// DeleteOp attempts to kill an operation by the id
func (o *Operations) DeleteOp(id string) error {
	o.mutex.Lock()
	op, ok := o.operations[id]
	o.mutex.Unlock()

	if ok {
		if err := o.Remove(op.id); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}
	return errors.Errorf("operation %q not found", id)
}

// WaitOp for an operation to be completed
func (o *Operations) WaitOp(id string, timeout time.Duration) (bool, error) {
	o.mutex.Lock()
	op, ok := o.operations[id]
	o.mutex.Unlock()

	if ok {
		return op.Wait(timeout)
	}
	return false, errors.Errorf("operation %q not found", id)
}

// Walk over the operations in a predictable manor
func (o *Operations) Walk(fn func(Op) error) error {
	o.mutex.Lock()

	var i int
	sorted := make([]Op, len(o.operations))
	for _, v := range o.operations {
		sorted[i] = v.Render()
		i++
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})

	o.mutex.Unlock()

	for _, v := range sorted {
		if err := fn(v); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

// Run returns a task function that performs operational GC checks against
// the internal cache of operations.
func (o *Operations) Run() (task.Func, task.Schedule) {
	operationsWrapper := func(ctx context.Context) {
		ch := make(chan struct{})
		go func() {
			o.run(ctx)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}

	schedule := task.Every(time.Duration(Interval) * time.Second)
	return operationsWrapper, schedule
}

func (o *Operations) run(ctx context.Context) {
	level.Debug(o.logger).Log("msg", "Starting operations gc")

	o.mutex.Lock()
	defer o.mutex.Unlock()

	for k, v := range o.operations {
		if v.IsFinal() {
			if err := o.Remove(k); err != nil {
				level.Error(o.logger).Log("msg", "failed to delete operation", "id", k, "err", err)
			}
		}
	}

	level.Debug(o.logger).Log("msg", "Finished operations gc")
}

package schedules

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/task"
	"github.com/spoke-d/thermionic/internal/tomb"
)

// Gateway mediates access to the dqlite cluster using a gRPC SQL client, and
// possibly runs a dqlite replica on this node (if we're configured to do
// so).
type Gateway interface {

	// IsDatabaseNode returns true if this gateway also run acts a raft database
	// node.
	IsDatabaseNode() bool

	// LeaderAddress returns the address of the current raft leader.
	LeaderAddress() (string, error)
}

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

	// SchemaVersion returns the underlying schema version for the cluster
	SchemaVersion() int

	// Close the database facade.
	Close() error
}

// Node mediates access to the data stored locally
type Node interface {
	database.DBAccessor
	db.NodeOpener
	db.NodeTransactioner

	// Dir returns the directory of the underlying database file.
	Dir() string

	// Close the database facade.
	Close() error
}

// Daemon can respond to requests from a shared client.
type Daemon interface {
	// Gateway returns the underlying Daemon Gateway
	Gateway() Gateway

	// Cluster returns the underlying Cluster
	Cluster() Cluster

	// Node returns the underlying Node associated with the daemon
	Node() Node

	// NodeConfigSchema returns the daemon schema for the local Node
	NodeConfigSchema() config.Schema
}

// Interval represents the number of seconds to wait between each schedule
// iteration
const Interval = 20

// Schedules represents a task collection of things that want to be
// run
type Schedules struct {
	daemon Daemon
	tasks  map[string]*Task
	mutex  sync.Mutex
	clock  clock.Clock
	logger log.Logger
}

// New creates a series of tasks to be worked on
func New(daemon Daemon, options ...Option) *Schedules {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Schedules{
		daemon: daemon,
		tasks:  make(map[string]*Task),
		clock:  opts.clock,
		logger: opts.logger,
	}
}

// Add an task to the collection
func (s *Schedules) Add(tsk *Task) error {
	if err := s.daemon.Cluster().Transaction(func(tx *db.ClusterTx) error {
		_, err := tx.TaskAdd(tsk.id, tsk.query, tsk.schedule.UTC().Unix(), tsk.status.Raw())
		return errors.WithStack(err)
	}); err != nil {
		return errors.Wrapf(err, "failed to add task %s to database", tsk.id)
	}

	s.mutex.Lock()
	s.tasks[tsk.id] = tsk
	s.mutex.Unlock()

	return nil
}

// Remove an task from the collection
func (s *Schedules) Remove(id string) error {
	if _, err := s.GetTsk(id); err != nil {
		return errors.WithStack(err)
	}

	if err := s.daemon.Cluster().Transaction(func(tx *db.ClusterTx) error {
		err := tx.TaskRemove(id)
		return errors.WithStack(err)
	}); err != nil {
		return errors.Wrapf(err, "failed to remove task %s to database", id)
	}

	s.mutex.Lock()
	delete(s.tasks, id)
	s.mutex.Unlock()

	return nil
}

// GetTsk returns a scheduled task
func (s *Schedules) GetTsk(id string) (Tsk, error) {
	s.mutex.Lock()
	task, ok := s.tasks[id]
	s.mutex.Unlock()
	if ok {
		return task.Render(), nil
	}

	// if it's not in the local cache, go fetch it
	if err := s.daemon.Cluster().Transaction(func(tx *db.ClusterTx) error {
		t, err := tx.TaskByUUID(id)
		if err == nil {
			task = &Task{
				id:       t.UUID,
				query:    t.Query,
				schedule: time.Unix(t.Schedule, 0),
				result:   t.Result,
				status:   Status(t.Status),
			}
		}
		return err
	}); err != nil {
		return Tsk{}, errors.Wrapf(err, "task not found for %q", id)
	}

	return task.Render(), nil
}

// Walk over the operations in a predictable manor
func (s *Schedules) Walk(fn func(Tsk) error) error {
	s.mutex.Lock()

	var tasks []db.Task
	if err := s.daemon.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		tasks, err = tx.Tasks()
		return err
	}); err != nil {
		s.mutex.Unlock()
		return errors.WithStack(err)
	}
	s.mutex.Unlock()

	for _, v := range tasks {
		status := Status(v.Status)
		if err := fn(Tsk{
			ID:         v.UUID,
			Query:      v.Query,
			Schedule:   time.Unix(v.Schedule, 0),
			Status:     status.String(),
			StatusCode: status,
			Result:     v.Result,
			URL:        fmt.Sprintf("/schedules/%s", v.UUID),
		}); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// Run returns a task function that performs operational GC checks against
// the internal cache of operations.
func (s *Schedules) Run() (task.Func, task.Schedule) {
	operationsWrapper := func(ctx context.Context) {
		ch := make(chan struct{})
		go func() {
			s.run(ctx)
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

func (s *Schedules) run(ctx context.Context) {
	// verify if we're clustered or not
	clustered, err := cluster.Enabled(s.daemon.Node())
	if err != nil {
		level.Error(s.logger).Log("msg", "cluster enabled", "err", err)
		return
	}
	if clustered {
		// Redirect all requests to the leader, which is the one with with
		// up-to-date knowledge of what nodes are part of the raft cluster.
		localAddress, err := node.HTTPSAddress(s.daemon.Node(), s.daemon.NodeConfigSchema())
		if err != nil {
			return
		}
		leader, err := s.daemon.Gateway().LeaderAddress()
		if err != nil {
			return
		}
		if localAddress != leader {
			// We're not the leader, so skip any thought of running the scheduler
			return
		}
	}

	// Note we're using the leader time here, if the leader is ever out of
	// sync, all chaos will occur
	now := s.clock.UTC().Add(-(time.Second * 30)).Round(time.Minute)

	var tasks []db.Task
	if err := s.daemon.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		tasks, err = tx.TasksByScheduleRange(now, now.Add(time.Minute), Pending.Raw())
		return err
	}); err != nil {
		return
	}

	start := s.clock.Now()
	s.runTasks(tasks)

	// If there is any time left, go and see if there are any pending tasks
	// that weren't actioned in the timeframe window. Only go back a couple of
	// interval times to ensure that we don't cause invalid results.
	intervalSeconds := time.Duration(Interval) * time.Second
	if s.clock.Now().Before(start.Add(intervalSeconds)) {
		if err := s.daemon.Cluster().Transaction(func(tx *db.ClusterTx) error {
			var err error
			from := intervalSeconds * 4
			tasks, err = tx.TasksByScheduleRange(now.Add(-from), now, Pending.Raw())
			return err
		}); err != nil {
			return
		}
		s.runTasks(tasks)
	}
}

func (s *Schedules) runTasks(tasks []db.Task) {
	if len(tasks) == 0 {
		level.Debug(s.logger).Log("msg", "no tasks to process")
		return
	}

	// Timeout is per query for the given interval.
	timeout := (Interval * time.Second) / time.Duration(len(tasks))
	database := s.daemon.Cluster().DB()

	for _, tsk := range tasks {
		func(tsk db.Task) {
			if tsk.Status != Pending.Raw() {
				return
			}

			ch := make(chan struct{}, 1)
			defer close(ch)

			level.Debug(s.logger).Log("msg", "processing task", "uuid", tsk.UUID)

			context, cancel := context.WithTimeout(context.Background(), timeout)
			catacomb, _ := tomb.WithContext(context)
			if err := catacomb.Go(func() error {
				err := s.query(database, tsk)
				ch <- struct{}{}
				return err
			}); err != nil {
				level.Error(s.logger).Log("msg", "error running query", "err", err)
			}

			select {
			case <-ch:
				level.Debug(s.logger).Log("msg", "processed task", "uuid", tsk.UUID)
				cancel()
			}
		}(tsk)
	}
}

func (s *Schedules) query(database database.DB, tsk db.Task) error {
	var batch Batch
	for _, query := range strings.Split(tsk.Query, ";") {
		q := strings.TrimLeft(query, " ")
		if q == "" {
			continue
		}

		tx, err := database.Begin()
		if err != nil {
			return err
		}

		var result Result
		if strings.HasPrefix(strings.ToUpper(query), "SELECT") {
			err = sqlSelect(tx, query, &result)
			tx.Rollback()
		} else {
			err = sqlExec(tx, query, &result)
			if err != nil {
				tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}
		if err != nil {
			batch.Err = err.Error()
			break
		}
		batch.Results = append(batch.Results, result)
	}

	status := Success
	if batch.Err != "" {
		status = Failure
	}
	bytes, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	if err := s.daemon.Cluster().Transaction(func(tx *db.ClusterTx) error {
		return tx.TaskUpdateResult(tsk.UUID, string(bytes), status.Raw())
	}); err != nil {
		return err
	}

	s.mutex.Lock()
	s.tasks[tsk.UUID] = &Task{
		id:       tsk.UUID,
		query:    tsk.Query,
		schedule: time.Unix(tsk.Schedule, 0),
		result:   string(bytes),
		err:      batch.Err,
		status:   status,
	}
	s.mutex.Unlock()
	return nil
}

func sqlSelect(tx database.Tx, query string, result *Result) error {
	result.Type = "select"

	rows, err := tx.Query(query)
	if err != nil {
		return errors.Wrap(err, "failed to execute query")
	}
	defer rows.Close()

	result.Columns, err = rows.Columns()
	if err != nil {
		return errors.Wrap(err, "failed to fetch column names")
	}

	for rows.Next() {
		row := make([]interface{}, len(result.Columns))
		rowPointers := make([]interface{}, len(result.Columns))
		for i := range row {
			rowPointers[i] = &row[i]
		}

		err := rows.Scan(rowPointers...)
		if err != nil {
			return errors.Wrap(err, "failed to scan row")
		}

		for i, column := range row {
			// Convert bytes to string. This is safe as
			// long as we don't have any BLOB column type.
			data, ok := column.([]byte)
			if ok {
				row[i] = string(data)
			}
		}

		result.Rows = append(result.Rows, row)
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "row error")
	}

	return nil
}

func sqlExec(tx database.Tx, query string, result *Result) error {
	result.Type = "exec"

	r, err := tx.Exec(query)
	if err != nil {
		return errors.Wrapf(err, "Failed to exec query")
	}

	result.RowsAffected, err = r.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "Failed to fetch affected rows")
	}
	return nil
}

// Batch holds all the query results for a singular query
type Batch struct {
	Results []Result `json:"results" yaml:"results"`
	Err     string   `json:"err" yaml:"err"`
}

// Result holds the query result for a singular query
type Result struct {
	Type         string          `json:"type" yaml:"type"`
	Columns      []string        `json:"columns" yaml:"columns"`
	Rows         [][]interface{} `json:"rows" yaml:"rows"`
	RowsAffected int64           `json:"rows_affected" yaml:"rows_affected"`
}

package schedules

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
)

// Tsk defines a very lightweight task
type Tsk struct {
	ID         string
	Query      string
	Schedule   time.Time
	Status     string
	StatusCode Status
	URL        string
	Result     string
	Err        string
}

// Task defines an operation to be run at a given schedule
type Task struct {
	id       string
	query    string
	schedule time.Time
	status   Status
	result   string
	err      string
	mutex    sync.RWMutex

	logger log.Logger
}

// NewTask creates an operation with sane defaults
func NewTask(id, query string, schedule time.Time, options ...Option) *Task {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Task{
		id:       id,
		query:    query,
		schedule: schedule,
		status:   Pending,
		logger:   opts.logger,
	}
}

// Render the task as a readonly Tsk
func (t *Task) Render() Tsk {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return Tsk{
		ID:         t.id,
		Query:      t.query,
		Schedule:   t.schedule,
		Status:     t.status.String(),
		StatusCode: t.status,
		Result:     t.result,
		URL:        fmt.Sprintf("/schedules/%s", t.id),
		Err:        t.err,
	}
}

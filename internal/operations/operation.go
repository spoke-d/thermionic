package operations

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
)

type class int

const (
	// Task represents a Operation classification
	Task class = iota
)

func (c class) String() string {
	switch c {
	case Task:
		return "task"
	default:
		return "unknown"
	}
}

// Op defines a very lightweight operation
type Op struct {
	ID         string
	Class      string
	Status     string
	StatusCode Status
	URL        string
	Err        string
}

// Changer notifies other listeners whom might be listening for
// operational changes
type Changer interface {
	// Dispatch an event to other nodes
	Change(Op)
}

// Operation defines a operation that can be run in the background
type Operation struct {
	id       string
	class    class
	status   Status
	err      string
	readOnly bool
	chanDone chan error
	mutex    sync.RWMutex

	hookRun    func(*Operation) error
	hookCancel func(*Operation) error

	logger  log.Logger
	changer Changer
}

// NewOperation creates an operation with sane defaults
func NewOperation(hook func(*Operation) error, changer Changer, options ...Option) *Operation {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &Operation{
		id:       uuid.NewRandom().String(),
		class:    Task,
		status:   Pending,
		chanDone: make(chan error),
		hookRun:  hook,
		changer:  changer,
		logger:   opts.logger,
	}
}

// Run the operation hook
func (o *Operation) Run() (<-chan error, error) {
	if o.status != Pending {
		return nil, errors.Errorf("only pending operations can be started")
	}

	chanRun := make(chan error, 1)

	o.setStatus(Running)

	if o.hookRun != nil {
		go o.runHook(chanRun, o.hookRun)
	} else {
		o.setStatus(Success)
		o.done()
		chanRun <- nil
	}

	return chanRun, nil
}

// Cancel the operation
func (o *Operation) Cancel() (<-chan error, error) {
	if o.status != Running {
		return nil, errors.Errorf("only running operations can be cancelled")
	}

	chanCancel := make(chan error, 1)

	o.setStatus(Cancelling)

	if o.hookCancel != nil {
		go o.cancelHook(chanCancel, o.hookCancel)
	} else {
		o.setStatus(Cancelled)
		o.done()
		chanCancel <- nil
	}

	return chanCancel, nil
}

// Wait for an operation to be completed
func (o *Operation) Wait(timeout time.Duration) (bool, error) {
	// check current state
	if o.status.IsFinal() {
		return true, nil
	}

	// wait indefinitely
	if timeout == -1 {
		select {
		case <-o.chanDone:
			return true, nil
		}
	}

	// Wait until timeout
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		select {
		case <-o.chanDone:
			return true, nil
		case <-timer.C:
			return false, errors.Errorf("timeout")
		}
	}
	return false, nil
}

// IsFinal returns the state of the operation
func (o *Operation) IsFinal() bool {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return o.status.IsFinal()
}

// Render the operation as a readonly Op
func (o *Operation) Render() Op {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return Op{
		ID:         o.id,
		Class:      o.class.String(),
		Status:     o.status.String(),
		StatusCode: o.status,
		URL:        fmt.Sprintf("/operations/%s", o.id),
		Err:        o.err,
	}
}

func (o *Operation) setStatus(status Status) {
	o.mutex.Lock()
	o.status = status
	o.mutex.Unlock()

	o.changer.Change(o.Render())
}

func (o *Operation) runHook(chanRun chan error, hook func(*Operation) error) {
	err := hook(o)
	if err != nil {
		level.Error(o.logger).Log("msg", "operation failure", "class", o.class, "id", o.id, "err", err)

		o.mutex.Lock()
		o.err = err.Error()
		o.mutex.Unlock()

		o.setStatus(Failure)

	} else {
		level.Debug(o.logger).Log("msg", "operation success", "class", o.class, "id", o.id)
		o.setStatus(Success)
	}
	o.done()
	chanRun <- err
}

func (o *Operation) cancelHook(chanCancel chan error, hook func(*Operation) error) {
	err := hook(o)
	if err != nil {
		level.Debug(o.logger).Log("msg", "cancelled operation failure", "class", o.class, "id", o.id, "err", err)

		o.setStatus(Running)
	} else {
		level.Debug(o.logger).Log("msg", "cancelled operation success", "class", o.class, "id", o.id)

		o.setStatus(Cancelled)
	}
	o.done()
	chanCancel <- err
}

func (o *Operation) done() {
	if o.readOnly {
		return
	}

	o.mutex.Lock()
	o.readOnly = true

	o.hookRun = nil
	o.hookCancel = nil

	close(o.chanDone)
	o.mutex.Unlock()
}

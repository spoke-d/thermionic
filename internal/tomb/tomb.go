package tomb

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrStillAlive = errors.New("still alive")
	ErrDying      = errors.New("dying")
)

// A Tomb tracks the lifecycle of one or more goroutines as alive,
// dying or dead, and the reason for their death.
//
// See the package documentation for details.
type Tomb struct {
	m      sync.Mutex
	alive  int
	dying  chan struct{}
	dead   chan struct{}
	reason error

	// context.Context is available in Go 1.7+.
	parent interface{}
	child  map[interface{}]childContext
}

type childContext struct {
	context interface{}
	cancel  func()
	done    <-chan struct{}
}

// New will create a Tomb with sane defaults
func New() *Tomb {
	return &Tomb{
		dead:   make(chan struct{}),
		dying:  make(chan struct{}),
		reason: ErrStillAlive,
	}
}

// Dead returns the channel that can be used to wait until
// all goroutines have finished running.
func (t *Tomb) Dead() <-chan struct{} {
	return t.dead
}

// Dying returns the channel that can be used to wait until
// t.Kill is called.
func (t *Tomb) Dying() <-chan struct{} {
	return t.dying
}

// Wait blocks until all goroutines have finished running, and
// then returns the reason for their death.
func (t *Tomb) Wait() error {
	<-t.dead

	t.m.Lock()
	reason := t.reason
	t.m.Unlock()

	return reason
}

// Go runs f in a new goroutine and tracks its termination.
//
// If f returns a non-nil error, t.Kill is called with that
// error as the death reason parameter.
//
// It is f's responsibility to monitor the tomb and return
// appropriately once it is in a dying state.
//
// It is safe for the f function to call the Go method again
// to create additional tracked goroutines. Once all tracked
// goroutines return, the Dead channel is closed and the
// Wait method unblocks and returns the death reason.
//
// Calling the Go method after all tracked goroutines return
// causes a runtime panic. For that reason, calling the Go
// method a second time out of a tracked goroutine is unsafe.
func (t *Tomb) Go(f func() error) error {
	t.m.Lock()
	defer t.m.Unlock()

	select {
	case <-t.dead:
		return errors.New("called after all goroutines terminated")
	default:
	}

	t.alive++
	go t.run(f)

	return nil
}

// Kill puts the tomb in a dying state for the given reason,
// closes the Dying channel, and sets Alive to false.
//
// Although Kill may be called multiple times, only the first
// non-nil error is recorded as the death reason.
//
// If reason is ErrDying, the previous reason isn't replaced
// even if nil. It's a runtime error to call Kill with ErrDying
// if t is not in a dying state.
func (t *Tomb) Kill(reason error) error {
	t.m.Lock()
	defer t.m.Unlock()

	return t.kill(reason)
}

// Killf calls the Kill method with an error built providing the received
// parameters to fmt.Errorf. The generated error is also returned.
func (t *Tomb) Killf(f string, a ...interface{}) error {
	err := fmt.Errorf(f, a...)
	t.Kill(err)
	return err
}

// Err returns the death reason, or ErrStillAlive if the tomb
// is not in a dying or dead state.
func (t *Tomb) Err() (reason error) {
	t.m.Lock()
	reason = t.reason
	t.m.Unlock()
	return
}

// Alive returns true if the tomb is not in a dying or dead state.
func (t *Tomb) Alive() bool {
	return t.Err() == ErrStillAlive
}

// Context returns a context that is a copy of the provided parent context with
// a replaced Done channel that is closed when either the tomb is dying or the
// parent is cancelled.
//
// If parent is nil, it defaults to the parent provided via WithContext, or an
// empty background parent if the tomb wasn't created via WithContext.
func (t *Tomb) Context(parent context.Context) context.Context {
	t.m.Lock()
	defer t.m.Unlock()

	if parent == nil {
		if t.parent == nil {
			t.parent = context.Background()
		}
		parent = t.parent.(context.Context)
	}

	if child, ok := t.child[parent]; ok {
		return child.context.(context.Context)
	}

	child, cancel := context.WithCancel(parent)
	t.addChild(parent, child, cancel)
	return child
}

func (t *Tomb) run(f func() error) {
	err := f()

	t.m.Lock()
	defer t.m.Unlock()

	t.alive--
	if t.alive == 0 || err != nil {
		t.kill(err)
		if t.alive == 0 {
			close(t.dead)
		}
	}
}

func (t *Tomb) kill(reason error) error {
	if reason == ErrStillAlive {
		return errors.New("kill with still alive")
	}
	if reason == ErrDying {
		if t.reason == ErrStillAlive {
			return errors.New("kill with dying while still alive")
		}
		return nil
	}
	if t.reason == ErrStillAlive {
		t.reason = reason
		close(t.dying)
		for _, child := range t.child {
			child.cancel()
		}
		t.child = nil
		return nil
	}
	if t.reason == nil {
		t.reason = reason
	}
	return nil
}

func (t *Tomb) addChild(parent, child context.Context, cancel func()) {
	if t.reason != ErrStillAlive {
		cancel()
		return
	}
	if t.child == nil {
		t.child = make(map[interface{}]childContext)
	}
	t.child[parent] = childContext{child, cancel, child.Done()}
	for parent, child := range t.child {
		select {
		case <-child.done:
			delete(t.child, parent)
		default:
		}
	}
}

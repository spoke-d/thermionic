package task

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Group of tasks sharing the same lifecycle.
//
// All tasks in a group will be started and stopped at the same time.
type Group struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	clock   Clock
	cancel  func()
	tasks   []Task
	running map[int]bool
}

// NewGroup creates a Group with sane defaults.
func NewGroup() *Group {
	return &Group{
		clock: wallClock{},
	}
}

// Add a new task to the group, returning its index.
func (g *Group) Add(f Func, schedule Schedule) *Task {
	task := Task{
		clock:    g.clock,
		f:        f,
		schedule: schedule,
		reset:    make(chan struct{}, 16), // Buffered to not block senders
	}
	g.tasks = append(g.tasks, task)
	return &task
}

// Len returns the number of tasks with in the group.
func (g *Group) Len() int {
	return len(g.tasks)
}

// Start all the tasks in the group.
func (g *Group) Start() {
	ctx := context.Background()
	ctx, g.cancel = context.WithCancel(ctx)

	g.wg.Add(len(g.tasks))

	g.running = make(map[int]bool)

	for i := range g.tasks {
		g.running[i] = true

		go func(task Task, i int) {
			task.loop(ctx)
			g.wg.Done()

			g.mu.Lock()
			g.running[i] = false
			g.mu.Unlock()
		}(g.tasks[i], i)
	}
}

// Stop all tasks in the group.
//
// This works by sending a cancellation signal to all tasks of the
// group and waiting for them to terminate.
//
// If a task is idle (i.e. not executing its task function) it will terminate
// immediately.
//
// If a task is busy executing its task function, the cancellation signal will
// propagate through the context passed to it, and the task will block waiting
// for the function to terminate.
//
// In case the given timeout expires before all tasks complete, this method
// exits immediately and returns an error, otherwise it returns nil.
func (g *Group) Stop(timeout time.Duration) error {
	if g.cancel == nil {
		// We were not even started
		return nil
	}
	g.cancel()

	graceful := make(chan struct{}, 1)
	go func() { g.wg.Wait(); close(graceful) }()

	// Wait for graceful termination, but abort if the context expires.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		var running []string
		for i, value := range g.running {
			if value {
				running = append(running, strconv.Itoa(i))
			}
		}
		return errors.Errorf("tasks %s are still running", strings.Join(running, ", "))
	case <-graceful:
		return nil
	}
}

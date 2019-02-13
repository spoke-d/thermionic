package task_test

import (
	"context"
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/task"
	"github.com/pkg/errors"
)

func TestGroupLen(t *testing.T) {
	group := task.NewGroup()

	ok := make(chan struct{})
	f := func(context.Context) { close(ok) }

	group.Add(f, task.Every(time.Second))
	group.Add(f, task.Every(time.Minute))

	if expected, actual := 2, group.Len(); expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestGroupStart(t *testing.T) {
	group := task.NewGroup()

	ok := make(chan struct{})
	f := func(context.Context) { close(ok) }

	group.Add(f, task.Every(time.Second))
	group.Start()

	select {
	case <-ok:
	case <-time.After(time.Second):
		t.Fatal("test expired")
	}

	err := group.Stop(time.Second)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestGroupStop(t *testing.T) {
	group := task.NewGroup()

	ok := make(chan struct{})
	defer close(ok)

	f := func(context.Context) {
		ok <- struct{}{}
		<-ok
	}

	group.Add(f, task.Every(time.Second))
	group.Start()

	select {
	case <-ok:
	case <-time.After(time.Second):
		t.Fatal("test expired")
	}

	err := group.Stop(time.Millisecond)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
	if expected, actual := "tasks 0 are still running", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestGroupStopWithNoStart(t *testing.T) {
	group := task.NewGroup()
	err := group.Stop(time.Second)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

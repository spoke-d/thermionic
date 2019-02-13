package task_test

import (
	"context"
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/task"
)

func TestStart(t *testing.T) {
	ok := make(chan struct{})
	f := func(context.Context) { close(ok) }

	stop, _ := task.Start(f, task.Every(time.Second))

	select {
	case <-ok:
	case <-time.After(time.Second):
		t.Fatal("test expired")
	}

	err := stop(time.Second)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

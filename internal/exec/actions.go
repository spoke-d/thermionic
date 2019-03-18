package exec

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
)

// Block creates a blocking executional group that waits until some other group
// action finishing, causing the closing of the group action.
func Block(g *Group) {
	cancel := make(chan struct{})
	g.Add(func() error {
		<-cancel
		return nil
	}, func(error) {
		close(cancel)
	})
}

// Interrupt creates a blocking executional group that becomes free if a
// interrupt or terminate os signal is received.
func Interrupt(g *Group) <-chan struct{} {
	cancel := make(chan struct{})
	g.Add(func() error {
		return interrupt(cancel)
	}, func(error) {
		close(cancel)
	})
	return cancel
}

func interrupt(cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	select {
	case sig := <-c:
		return errors.Errorf("received signal %s", sig)
	case <-cancel:
		return errors.New("canceled")
	}
}

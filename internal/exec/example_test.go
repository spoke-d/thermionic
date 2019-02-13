package exec_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/spoke-d/thermionic/internal/exec"
)

func ExampleGroup_Add_basic() {
	var g exec.Group
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			select {
			case <-time.After(time.Second):
				fmt.Println("The first actor had its time elapsed")
				return nil
			case <-cancel:
				fmt.Println("The first actor was canceled")
				return nil
			}
		}, func(err error) {
			fmt.Printf("The first actor was interrupted with: %v\n", err)
			close(cancel)
		})
	}
	{
		g.Add(func() error {
			fmt.Println("The second actor is returning immediately")
			return errors.New("immediate teardown")
		}, func(err error) {
			// Note that this interrupt function is called even though the
			// corresponding execute function has already returned.
			fmt.Printf("The second actor was interrupted with: %v\n", err)
		})
	}
	fmt.Printf("The group was terminated with: %v\n", g.Run())
	// Output:
	// The second actor is returning immediately
	// The first actor was interrupted with: immediate teardown
	// The second actor was interrupted with: immediate teardown
	// The first actor was canceled
	// The group was terminated with: immediate teardown
}

func ExampleGroup_Add_context() {
	runUntilCanceled := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	var g exec.Group
	{
		ctx, cancel := context.WithCancel(ctx) // note: shadowed
		g.Add(func() error {
			return runUntilCanceled(ctx)
		}, func(error) {
			cancel()
		})
	}
	go cancel()
	fmt.Printf("The group was terminated with: %v\n", g.Run())
	// Output:
	// The group was terminated with: context canceled
}

func ExampleGroup_Add_listener() {
	var g exec.Group
	{
		ln, _ := net.Listen("tcp", ":0")
		g.Add(func() error {
			defer fmt.Println("http.Serve returned")
			return http.Serve(ln, http.NewServeMux())
		}, func(error) {
			ln.Close()
		})
	}
	{
		g.Add(func() error {
			return errors.New("immediate teardown")
		}, func(error) {
			// Nothing.
		})
	}
}

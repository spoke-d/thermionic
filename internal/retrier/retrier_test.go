package retrier_test

import (
	"fmt"
	"time"

	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/retrier"
)

func Example() {
	retry := retrier.New(clock.DefaultSleeper, 10, time.Second)
	err := retry.Run(func() error {
		return nil
	})

	switch {
	case err == nil:
		fmt.Println("success!")
	case retrier.ErrRetry(err):
		fmt.Println("deadline timeout")
	default:
		fmt.Println("other error")
	}

	// Output:
	// success!
}

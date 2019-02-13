package retrier

import (
	"time"

	"github.com/spoke-d/thermionic/internal/clock"
)

// Retrier encapsulates the mechanism around retrying function
type Retrier struct {
	sleeper clock.Sleeper
	backoff []time.Duration
}

// New creates a Retrier with sane defaults
func New(sleeper clock.Sleeper, amount int, duration time.Duration) *Retrier {
	return &Retrier{
		sleeper: sleeper,
		backoff: linear(amount, duration),
	}
}

// Run executes the given function
func (r *Retrier) Run(fn func() error) error {
	var retries int
	for {
		err := fn()
		if err == nil {
			return nil
		}

		if retries >= len(r.backoff) {
			return errRetry{err}
		}
		r.sleeper.Sleep(r.backoff[retries])
		retries++
	}
}

func exponential(n int, d time.Duration) []time.Duration {
	res := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		res[i] = d
		d *= 2
	}
	return res
}

func linear(n int, d time.Duration) []time.Duration {
	res := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		res[i] = d
	}
	return res
}

type errRetry struct {
	err error
}

func (e errRetry) Error() string {
	return e.err.Error()
}

// ErrRetry checks if the error was because of too many retries.
func ErrRetry(err error) bool {
	_, ok := err.(errRetry)
	return ok
}

package clock

import "time"

// Sleeper defines a interface that handles sleeping at the right times
type Sleeper interface {

	// Sleep pauses the current goroutine for at least the duration d.
	// A negative or zero duration causes Sleep to return immediately.
	Sleep(time.Duration)
}

// DefaultSleeper exports a sleeper so that the retrier can backoff at
// the right times. The reason for it's inclusion as a dependency is to
// allow mocking to work in the correct way, without any delays.
var DefaultSleeper = sleeper{}

type sleeper struct{}

func (sleeper) Sleep(d time.Duration) {
	time.Sleep(d)
}

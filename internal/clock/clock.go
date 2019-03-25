package clock

import (
	"time"
)

// Clock defines an interface for Clocks
type Clock interface {

	// Now returns the current local time.
	Now() time.Time

	// UTC returns the time now as UTC
	UTC() time.Time

	// After waits for the duration to elapse and then sends the current time
	// on the returned channel.
	// It is equivalent to NewTimer(d).C.
	// The underlying Timer is not recovered by the garbage collector
	// until the timer fires. If efficiency is a concern, use NewTimer
	// instead and call Timer.Stop if the timer is no longer needed.
	After(d time.Duration) <-chan time.Time
}

// WallClock represents an implementation of Clock, that uses the wall time.
type WallClock struct{}

// New creates a new Clock
func New() WallClock {
	return WallClock{}
}

// Now returns the current local time.
func (WallClock) Now() time.Time {
	return time.Now()
}

// UTC returns the current utc time.
func (WallClock) UTC() time.Time {
	return time.Now().UTC()
}

// After waits for the duration to elapse and then sends the current time
// on the returned channel.
// It is equivalent to NewTimer(d).C.
// The underlying Timer is not recovered by the garbage collector
// until the timer fires. If efficiency is a concern, use NewTimer
// instead and call Timer.Stop if the timer is no longer needed.
func (WallClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

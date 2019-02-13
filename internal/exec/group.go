package exec

// Group collects actions (functions) and runs them concurrently. When one action
// (function) returns, all actions are interrupted.
// The zero value of a Group is useful.
type Group struct {
	actions []action
}

// NewGroup creates a new group
func NewGroup() *Group {
	return &Group{}
}

// Add an action (function) to the group. Each action must be pre-emptable by an
// interrupt function. That is, if interrupt is invoked, execute should return.
// Also is must be safe to call interrupt even after execute has returned.
//
// The first action (function) to return interrupts all running actions.
// The error is passed to the interrupt functions and is returned by Run.
func (g *Group) Add(execute func() error, interrupt func(error)) {
	g.actions = append(g.actions, action{execute, interrupt})
}

// Run all actions (functions) concurrently.
// When the first action returns, all others are interrupted.
// Run only returns when all actions have exited.
// Run returns the error returned by the first existing action.
func (g *Group) Run() error {
	if len(g.actions) == 0 {
		return nil
	}

	// Run each action.
	errors := make(chan error, len(g.actions))
	for _, a := range g.actions {
		go func(a action) {
			errors <- a.execute()
		}(a)
	}

	// Wait for the first action to stop.
	err := <-errors

	// Signal all actions to stop.
	for _, a := range g.actions {
		a.interrupt(err)
	}

	// Wait for all actions to stop.
	for i := 1; i < cap(errors); i++ {
		<-errors
	}

	// Return the original error.
	return err
}

type action struct {
	execute   func() error
	interrupt func(error)
}

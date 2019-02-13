package cluster

// Rexport the hook and check functions for testing.
var (
	Hook  = hook
	Check = check
)

type Context = hookContext

package operations

type Status int

const (
	// Pending status operation
	Pending Status = iota
	// Running status operation
	Running
	// Cancelling status operation
	Cancelling
	// Failure status operation
	Failure
	// Success status operation
	Success
	// Cancelled status operation
	Cancelled
)

// Raw returns the underlying value
func (s Status) Raw() int {
	return int(s)
}

// IsFinal will return true if the status code indicates an end state
func (s Status) IsFinal() bool {
	return s == Failure || s == Success || s == Cancelled
}

func (s Status) String() string {
	switch s {
	case Pending:
		return "pending"
	case Running:
		return "running"
	case Cancelling:
		return "cancelling"
	case Failure:
		return "failure"
	case Success:
		return "success"
	case Cancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

package schedules

// Status defines all the state a schedule task can be in
type Status int

const (
	// Pending status operation
	Pending Status = iota
	// Running status operation
	Running
	// Failure status operation
	Failure
	// Success status operation
	Success
)

// Raw returns the underlying value
func (s Status) Raw() int {
	return int(s)
}

// IsFinal will return true if the status code indicates an end state
func (s Status) IsFinal() bool {
	return s == Failure || s == Success
}

func (s Status) String() string {
	switch s {
	case Pending:
		return "pending"
	case Running:
		return "running"
	case Failure:
		return "failure"
	case Success:
		return "success"
	default:
		return "unknown"
	}
}

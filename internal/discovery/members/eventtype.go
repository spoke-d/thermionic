package members

// EventType is the potential event type for member event
type EventType int

const (
	// EventMember was received from the cluster
	EventMember EventType = iota

	// EventQuery was received from the cluster
	EventQuery

	// EventUser was received from the cluster
	EventUser

	// EventError was received from the cluster
	EventError
)

// MemberEventType is the potential event type for member event
type MemberEventType int

const (
	// EventMemberJoined notified from a cluster when a member has joined
	EventMemberJoined MemberEventType = iota

	// EventMemberLeft notified from a cluster when a member has left
	EventMemberLeft

	// EventMemberFailed notified from a cluster when a member has failed
	EventMemberFailed

	// EventMemberUpdated notified from a cluster when a member has updated
	EventMemberUpdated
)

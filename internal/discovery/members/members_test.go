package members_test

import (
	"reflect"
	"testing"
	"testing/quick"

	"github.com/spoke-d/thermionic/internal/discovery/members"
	"github.com/pkg/errors"
)

func TestMembersEvent(t *testing.T) {
	t.Parallel()

	t.Run("creation", func(t *testing.T) {
		var (
			mems = []members.Member{{}}
			evt  = members.NewMemberEvent(members.EventMemberJoined, mems)
			real = evt.(*members.MemberEvent)
		)

		if expected, actual := members.EventMemberJoined, real.EventType; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := mems, real.Members; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := members.EventMember, real.Type(); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestUserEvent(t *testing.T) {
	t.Parallel()

	t.Run("creation", func(t *testing.T) {
		fn := func(a string, b []byte) bool {
			var (
				evt  = members.NewUserEvent(a, b)
				real = evt.(*members.UserEvent)
			)

			if expected, actual := a, real.Name; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := b, real.Payload; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := members.EventUser, real.Type(); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestQueryEvent(t *testing.T) {
	t.Parallel()

	t.Run("creation", func(t *testing.T) {
		fn := func(a string, b []byte) bool {
			var (
				evt  = members.NewQueryEvent(a, b, nil)
				real = evt.(*members.QueryEvent)
			)

			if expected, actual := a, real.Name; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := b, real.Payload; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := members.EventQuery, real.Type(); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestErrorEvent(t *testing.T) {
	t.Parallel()

	t.Run("creation", func(t *testing.T) {
		fn := func(a string) bool {
			var (
				evt  = members.NewErrorEvent(errors.Errorf("%s", a))
				real = evt.(*members.ErrorEvent)
			)

			if expected, actual := a, real.Error.Error(); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := members.EventError, real.Type(); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

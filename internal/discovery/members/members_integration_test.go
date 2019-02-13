// +build integration

package members_test

import (
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/spoke-d/thermionic/internal/discovery/members"
	"github.com/pborman/uuid"
)

func TestRealMembers_Integration(t *testing.T) {
	t.Parallel()

	config := []members.Config{
		members.WithDaemon("0.0.0.0", "xxx-yyy-zzz"),
		members.WithBindAddrPort("0.0.0.0", 8080),
		members.WithLogOutput(ioutil.Discard),
		members.WithNodeName("peer"),
	}

	t.Run("new", func(t *testing.T) {
		mbrs := members.NewMembers()
		if err := mbrs.Init(config...); err != nil {
			t.Fatal(err)
		}

		mbrs.Shutdown()

		if expected, actual := false, mbrs == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("join", func(t *testing.T) {
		mbrs := members.NewMembers()
		if err := mbrs.Init(config...); err != nil {
			t.Fatal(err)
		}

		defer mbrs.Shutdown()

		a, err := mbrs.Join()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 0, a; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("leave", func(t *testing.T) {
		mbrs := members.NewMembers()
		if err := mbrs.Init(config...); err != nil {
			t.Fatal(err)
		}

		defer mbrs.Shutdown()

		a, err := mbrs.Join()
		if err != nil {
			t.Fatal(err)
		}

		err = mbrs.Leave()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 0, a; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("member list", func(t *testing.T) {
		mbrs := members.NewMembers()
		if err := mbrs.Init(config...); err != nil {
			t.Fatal(err)
		}
		defer mbrs.Shutdown()

		if _, err := mbrs.Join(); err != nil {
			t.Fatal(err)
		}

		m := mbrs.MemberList()
		if expected, actual := false, m == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("walk", func(t *testing.T) {
		mbrs := members.NewMembers()
		if err := mbrs.Init(config...); err != nil {
			t.Fatal(err)
		}
		defer mbrs.Shutdown()

		if _, err := mbrs.Join(); err != nil {
			t.Fatal(err)
		}

		var got []members.PeerInfo
		if err := mbrs.Walk(func(info members.PeerInfo) error {
			got = append(got, info)
			return nil
		}); err != nil {
			t.Error(err)
		}

		want := []members.PeerInfo{
			{
				Type:          members.PeerType(""),
				Name:          "peer",
				DaemonAddress: "0.0.0.0",
				DaemonNonce:   "xxx-yyy-zzz",
			},
		}
		if expected, actual := want, got; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		mbrs := members.NewMembers()
		if err := mbrs.Init(config...); err != nil {
			t.Fatal(err)
		}

		err := mbrs.Shutdown()
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestRealMemberList(t *testing.T) {
	t.Parallel()

	id := uuid.NewRandom().String()

	config := []members.Config{
		members.WithDaemon("0.0.0.0", "xxx-yyy-zzz"),
		members.WithBindAddrPort("0.0.0.0", 8082),
		members.WithLogOutput(ioutil.Discard),
		members.WithNodeName(id),
	}

	t.Run("number of members", func(t *testing.T) {
		mbrs := members.NewMembers()
		if err := mbrs.Init(config...); err != nil {
			t.Fatal(err)
		}
		defer mbrs.Shutdown()

		amount := mbrs.MemberList().NumMembers()
		if expected, actual := 1, amount; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("local node name", func(t *testing.T) {
		mbrs := members.NewMembers()
		if err := mbrs.Init(config...); err != nil {
			t.Fatal(err)
		}
		defer mbrs.Shutdown()

		name := mbrs.MemberList().LocalNode().Name()
		if expected, actual := id, name; expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})

	t.Run("members", func(t *testing.T) {
		mbrs := members.NewMembers()
		if err := mbrs.Init(config...); err != nil {
			t.Fatal(err)
		}
		defer mbrs.Shutdown()

		m := mbrs.MemberList().Members()
		if expected, actual := 1, len(m); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}

package ioext

import (
	"io/ioutil"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"
)

func TestOffsetReader(t *testing.T) {
	t.Parallel()

	fn := func(s ASCII) bool {
		r := OffsetReader(strings.NewReader(s.String()), 3)

		data, err := ioutil.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := string(data), string(s[3:]); expected != actual {
			t.Errorf("expected %q, actual %q", expected, actual)
		}

		return true
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

// ASCII creates a series of tags that are ascii compliant.
type ASCII []byte

// Generate allows ASCII to be used within quickcheck scenarios.
func (ASCII) Generate(r *rand.Rand, size int) reflect.Value {
	var (
		chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		res   = make([]byte, size+10)
	)

	for k := range res {
		res[k] = byte(chars[r.Intn(len(chars)-1)])
	}

	return reflect.ValueOf(res)
}

func (a ASCII) Slice() []byte {
	return a
}

func (a ASCII) String() string {
	return string(a)
}

package radix_test

import (
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/spoke-d/thermionic/internal/clui/radix"
)

func TestRadix(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		fn := func(k string) bool {
			tree := radix.New()
			_, ok := tree.Get(k)
			return !ok
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("insertion", func(t *testing.T) {
		fn := func(k, v string) bool {
			tree := radix.New()
			got, ok, err := tree.Insert(k, v)
			if err != nil {
				t.Fatal(err)
			}
			if expected, actual := true, got == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := false, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			got, ok = tree.Get(k)
			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if expected, actual := v, got; expected != actual {
				t.Errorf("expected: %s, actual: %s", expected, actual)
			}
			return ok
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("multiple insertion", func(t *testing.T) {
		fn := func(pairs []pair) bool {
			// make sure they're all unique
			pairs = uniquePairs(t, pairs)

			tree := radix.New()

			for _, p := range pairs {
				got, ok, err := tree.Insert(p.K, p.V)
				if err != nil {
					t.Fatal(err)
				}
				if expected, actual := true, got == nil; expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
				if expected, actual := false, ok; expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
			}

			for _, p := range pairs {
				got, ok := tree.Get(p.K)
				if expected, actual := true, ok; expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
				if expected, actual := p.V, got; expected != actual {
					t.Errorf("expected: %s, actual: %s", expected, actual)
				}
				if !ok {
					return false
				}
			}
			return tree.Len() == len(pairs)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		fn := func(k, v string) bool {
			tree := radix.New()
			got, ok, err := tree.Insert(k, v)
			if err != nil {
				t.Fatal(err)
			}
			if expected, actual := true, got == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := false, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			got, ok = tree.Delete(k)
			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := v, got; expected != actual {
				t.Errorf("expected: %s, actual: %s", expected, actual)
			}
			_, ok = tree.Get(k)
			return !ok
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("multiple deletion", func(t *testing.T) {
		fn := func(pairs []pair) bool {
			// make sure they're all unique
			pairs = uniquePairs(t, pairs)

			tree := radix.New()

			for _, p := range pairs {
				got, ok, err := tree.Insert(p.K, p.V)
				if err != nil {
					t.Fatal(err)
				}
				if expected, actual := true, got == nil; expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
				if expected, actual := false, ok; expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
			}

			for _, p := range pairs {
				got, ok := tree.Delete(p.K)
				if expected, actual := true, ok; expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
				if expected, actual := p.V, got; expected != actual {
					t.Errorf("expected: %s, actual: %s", expected, actual)
				}
				if !ok {
					return false
				}
			}
			return tree.Len() == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("walk", func(t *testing.T) {
		fn := func(pairs []pair) bool {
			// make sure they're all unique
			pairs = uniquePairs(t, pairs)

			tree := radix.New()

			for _, p := range pairs {
				got, ok, err := tree.Insert(p.K, p.V)
				if err != nil {
					t.Fatal(err)
				}
				if expected, actual := true, got == nil; expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
				if expected, actual := false, ok; expected != actual {
					t.Errorf("expected: %t, actual: %t", expected, actual)
				}
			}

			var w []walked
			tree.Walk(func(k string, v radix.Value) bool {
				w = append(w, walked{k, v})
				return false
			})
			return matchedAll(t, pairs, w)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestRadixRoot(t *testing.T) {
	t.Parallel()

	t.Run("insert", func(t *testing.T) {
		tree := radix.New()
		_, ok, err := tree.Insert("", "")
		if err != nil {
			t.Fatal(err)
		}
		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("delete", func(t *testing.T) {
		tree := radix.New()
		_, ok := tree.Delete("")
		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("get", func(t *testing.T) {
		tree := radix.New()
		_, ok := tree.Get("")
		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestRadixLongestPrefix(t *testing.T) {
	tree := radix.New()

	for _, k := range []string{
		"",
		"foo",
		"foobar",
		"foobarbaz",
		"foobarbazzip",
		"foozip",
	} {
		tree.Insert(k, "")
	}

	for _, tc := range []struct {
		match         bool
		input, output string
	}{
		{true, "a", ""},
		{true, "abc", ""},
		{true, "fo", ""},
		{true, "foo", "foo"},
		{true, "foob", "foo"},
		{true, "foobar", "foobar"},
		{true, "foobarba", "foobar"},
		{true, "foobarbaz", "foobarbaz"},
		{true, "foobarbazzi", "foobarbaz"},
		{true, "foobarbazzip", "foobarbazzip"},
		{true, "foozi", "foo"},
		{true, "foozip", "foozip"},
		{true, "foozipzap", "foozip"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			v, _, ok := tree.LongestPrefix(tc.input)
			if tc.match != ok {
				t.Errorf("expected: %t, actual: %t", tc.match, ok)
			}
			if tc.output != v {
				t.Errorf("expected: %s, actual: %s", tc.output, v)
			}
		})
	}
}

func TestRadixWalkPrefix(t *testing.T) {
	t.Parallel()

	tree := radix.New()

	for _, k := range []string{
		"foobar",
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"zipzap",
	} {
		tree.Insert(k, "")
	}

	for _, tc := range []struct {
		input  string
		output []string
	}{
		{
			"f",
			[]string{"foobar", "foo/bar/baz", "foo/baz/bar", "foo/zip/zap"},
		},
		{
			"foo",
			[]string{"foobar", "foo/bar/baz", "foo/baz/bar", "foo/zip/zap"},
		},
		{
			"foob",
			[]string{"foobar"},
		},
		{
			"foo/",
			[]string{"foo/bar/baz", "foo/baz/bar", "foo/zip/zap"},
		},
		{
			"foo/b",
			[]string{"foo/bar/baz", "foo/baz/bar"},
		},
		{
			"foo/ba",
			[]string{"foo/bar/baz", "foo/baz/bar"},
		},
		{
			"foo/bar",
			[]string{"foo/bar/baz"},
		},
		{
			"foo/bar/baz",
			[]string{"foo/bar/baz"},
		},
		{
			"foo/bar/bazoo",
			[]string{},
		},
		{
			"z",
			[]string{"zipzap"},
		},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got := make([]string, 0)
			tree.WalkPrefix(tc.input, func(s string, v radix.Value) bool {
				got = append(got, s)
				return false
			})

			sort.Strings(got)
			sort.Strings(tc.output)

			if expected, actual := tc.output, got; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
		})
	}
}

type pair struct {
	K, V string
}

type walked struct {
	K string
	V radix.Value
}

func uniquePairs(t *testing.T, p []pair) []pair {
	t.Helper()

	m := make(map[string]pair)
	for _, v := range p {
		m[v.K] = v
	}

	var r []pair
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

func matchedAll(t *testing.T, p []pair, w []walked) bool {
	t.Helper()

	if len(p) != len(w) {
		return false
	}

	m := make(map[string]string)
	for _, v := range p {
		m[v.K] = v.V
	}

	for _, v := range w {
		x, ok := m[v.K]
		if !ok || v.V.(string) != x {
			return false
		}
	}
	return true
}

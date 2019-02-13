package query_test

import (
	"testing"

	"github.com/spoke-d/thermionic/internal/db/query"
)

func TestParams(t *testing.T) {
	for _, test := range []struct {
		name   string
		amount int
		result string
	}{
		{
			name:   "empty",
			amount: 0,
			result: "()",
		},
		{
			name:   "many",
			amount: 5,
			result: "(?, ?, ?, ?, ?)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := query.Params(test.amount)
			if expected, actual := test.result, got; expected != actual {
				t.Errorf("expected: %s, actual: %s", expected, actual)
			}
		})
	}
}

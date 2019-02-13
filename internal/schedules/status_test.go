package schedules_test

import (
	"fmt"
	"testing"

	"github.com/spoke-d/thermionic/internal/schedules"
)

func TestIsFinal(t *testing.T) {
	t.Parallel()

	for k, tc := range []struct {
		Status   schedules.Status
		Expected bool
	}{
		{
			Status:   schedules.Pending,
			Expected: false,
		},
		{
			Status:   schedules.Running,
			Expected: false,
		},
		{
			Status:   schedules.Failure,
			Expected: true,
		},
		{
			Status:   schedules.Success,
			Expected: true,
		},
	} {
		t.Run(fmt.Sprintf("running %v with status %v", k, tc.Status.Raw()), func(t *testing.T) {
			if expected, actual := tc.Expected, tc.Status.IsFinal(); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
		})
	}
}

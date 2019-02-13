package operations_test

import (
	"fmt"
	"testing"

	"github.com/spoke-d/thermionic/internal/operations"
)

func TestIsFinal(t *testing.T) {
	t.Parallel()

	for k, tc := range []struct {
		Status   operations.Status
		Expected bool
	}{
		{
			Status:   operations.Pending,
			Expected: false,
		},
		{
			Status:   operations.Running,
			Expected: false,
		},
		{
			Status:   operations.Cancelling,
			Expected: false,
		},
		{
			Status:   operations.Failure,
			Expected: true,
		},
		{
			Status:   operations.Success,
			Expected: true,
		},
		{
			Status:   operations.Cancelled,
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

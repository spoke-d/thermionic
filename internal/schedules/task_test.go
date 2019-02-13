package schedules_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/schedules"
	"github.com/pborman/uuid"
)

func TestTaskRender(t *testing.T) {
	t.Parallel()

	uuid := uuid.NewRandom().String()
	query := "SELECT * FROM nodes"
	timestamp := time.Now()
	task := schedules.NewTask(uuid, query, timestamp)

	tsk := task.Render()
	want := schedules.Tsk{
		ID:         uuid,
		Query:      query,
		Schedule:   timestamp,
		Status:     schedules.Pending.String(),
		StatusCode: schedules.Pending,
		Result:     "",
		URL:        fmt.Sprintf("/schedules/%s", uuid),
	}

	if expected, actual := want, tsk; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

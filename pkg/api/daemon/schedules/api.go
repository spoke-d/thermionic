package schedules

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/spoke-d/thermionic/internal/schedules"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/go-kit/kit/log"
	"github.com/pborman/uuid"
)

// Task defines a very lightweight task
type Task struct {
	ID         string    `json:"id" yaml:"id"`
	URL        string    `json:"url" yaml:"url"`
	Query      string    `json:"query" yaml:"query"`
	Schedule   time.Time `json:"schedule" yaml:"schedule"`
	Status     string    `json:"status" yaml:"status"`
	StatusCode int       `json:"status_code" yaml:"status_code"`
	Result     string    `json:"result" yaml:"result"`
	Err        string    `json:"err" yaml:"err"`
}

// API defines a operations API
type API struct {
	api.DefaultService
	name   string
	logger log.Logger
}

// NewAPI creates a API with sane defaults
func NewAPI(name string, options ...Option) *API {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &API{
		name:   name,
		logger: opts.logger,
	}
}

// Name returns the API name
func (a *API) Name() string {
	return a.name
}

// Get defines a service for calling "GET" method and returns a response.
func (a *API) Get(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	tasks := make(map[string]Task)
	if err := d.Schedules().Walk(func(tsk schedules.Tsk) error {
		tasks[tsk.ID] = Task{
			ID:         tsk.ID,
			URL:        fmt.Sprintf("/%s%s", d.Version(), tsk.URL),
			Query:      tsk.Query,
			Schedule:   tsk.Schedule,
			Status:     tsk.Status,
			StatusCode: tsk.StatusCode.Raw(),
			Result:     tsk.Result,
			Err:        tsk.Err,
		}
		return nil
	}); err != nil {
		return api.SmartError(err)
	}
	result := make([]Task, len(tasks))
	var i int
	for _, v := range tasks {
		result[i] = v
		i++
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Schedule.Before(result[j].Schedule)
	})
	return api.SyncResponse(true, result)
}

// Post defines a service for calling "POST" method and returns a response.
func (a *API) Post(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	var info ScheduleTaskRequest
	if err := json.NewDecoder(req.Body).Decode(&info); err != nil {
		return api.BadRequest(err)
	}

	task := schedules.NewTask(uuid.NewRandom().String(), info.Query, info.Schedule)
	if err := d.Schedules().Add(task); err != nil {
		return api.SmartError(err)
	}

	tsk := task.Render()
	return api.SyncResponse(true, Task{
		ID:         tsk.ID,
		URL:        fmt.Sprintf("/%s%s", d.Version(), tsk.URL),
		Query:      tsk.Query,
		Schedule:   tsk.Schedule,
		Status:     tsk.Status,
		StatusCode: tsk.StatusCode.Raw(),
		Result:     tsk.Result,
		Err:        tsk.Err,
	})
}

// ScheduleTaskRequest represents a new task
type ScheduleTaskRequest struct {
	Query    string    `json:"query" yaml:"query"`
	Schedule time.Time `json:"schedule" yaml:"schedule"`
}

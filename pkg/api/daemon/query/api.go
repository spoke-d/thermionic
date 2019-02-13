package query

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/pkg/errors"
)

// API defines a query API
type API struct {
	api.DefaultService
	name string
}

// NewAPI creates a API with sane defaults
func NewAPI(name string) *API {
	return &API{
		name: name,
	}
}

// Name returns the API name
func (a *API) Name() string {
	return a.name
}

// Post defines a service for calling "POST" method and returns a response.
func (a *API) Post(ctx context.Context, req *http.Request) api.Response {
	defer req.Body.Close()

	d, err := api.GetDaemon(ctx)
	if err != nil {
		return api.InternalError(err)
	}

	// Parse the request.
	var body Query
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return api.BadRequest(err)
	}

	if !contains([]string{"local", "global"}, body.Database) {
		return api.BadRequest(errors.Errorf("invalid database"))
	}
	if body.Query == "" {
		return api.BadRequest(errors.Errorf("No query provided"))
	}

	var db database.DB
	if body.Database == "global" {
		db = d.Cluster().DB()
	} else {
		db = d.Node().DB()
	}

	var batch Batch
	for _, query := range strings.Split(body.Query, ";") {
		q := strings.TrimLeft(query, " ")
		if q == "" {
			continue
		}

		tx, err := db.Begin()
		if err != nil {
			return api.SmartError(err)
		}

		var result Result
		if strings.HasPrefix(strings.ToUpper(q), "SELECT") {
			err = sqlSelect(tx, q, &result)
			tx.Rollback()
		} else {
			err = sqlExec(tx, q, &result)
			if err != nil {
				tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}
		if err != nil {
			return api.SmartError(err)
		}
		batch.Results = append(batch.Results, result)
	}

	return api.SyncResponse(true, batch)
}

func sqlSelect(tx database.Tx, query string, result *Result) error {
	result.Type = "select"

	rows, err := tx.Query(query)
	if err != nil {
		return errors.Wrap(err, "failed to execute query")
	}
	defer rows.Close()

	result.Columns, err = rows.Columns()
	if err != nil {
		return errors.Wrap(err, "failed to fetch column names")
	}

	for rows.Next() {
		row := make([]interface{}, len(result.Columns))
		rowPointers := make([]interface{}, len(result.Columns))
		for i := range row {
			rowPointers[i] = &row[i]
		}

		err := rows.Scan(rowPointers...)
		if err != nil {
			return errors.Wrap(err, "failed to scan row")
		}

		for i, column := range row {
			// Convert bytes to string. This is safe as
			// long as we don't have any BLOB column type.
			data, ok := column.([]byte)
			if ok {
				row[i] = string(data)
			}
		}

		result.Rows = append(result.Rows, row)
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "row error")
	}

	return nil
}

func sqlExec(tx database.Tx, query string, result *Result) error {
	result.Type = "exec"

	r, err := tx.Exec(query)
	if err != nil {
		return errors.Wrapf(err, "failed to exec query")
	}

	result.RowsAffected, err = r.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to fetch affected rows")
	}
	return nil
}

func contains(a []string, b string) bool {
	for _, v := range a {
		if v == b {
			return true
		}
	}
	return false
}

type Query struct {
	Database string `json:"database" yaml:"database"`
	Query    string `json:"query" yaml:"query"`
}

type Batch struct {
	Results []Result `json:"results" yaml:"results"`
}

type Result struct {
	Type         string          `json:"type" yaml:"type"`
	Columns      []string        `json:"columns" yaml:"columns"`
	Rows         [][]interface{} `json:"rows" yaml:"rows"`
	RowsAffected int64           `json:"rows_affected" yaml:"rows_affected"`
}

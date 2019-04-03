package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/pkg/api/daemon/schedules"
	yaml "gopkg.in/yaml.v2"
)

type scheduleAddCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	format     string
	serverCert string
	clientCert string
	clientKey  string
}

// NewScheduleAddCmd creates a Command with sane defaults
func NewScheduleAddCmd(ui clui.UI) clui.Command {
	c := &scheduleAddCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("schedule", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *scheduleAddCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.format, "format", "yaml", "format to output the information json|yaml|tabular")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *scheduleAddCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *scheduleAddCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *scheduleAddCmd) Help() string {
	return `
Usage: 

  schedule [flags] add <time> <query>

Description:

  Add a schedule to be run in the background. The
  result of the schedule task will be saved to the
  task result directly.

  The task scheduled will be run on the leader node
  if clustered, or alternatively run on the singular
  node.

  The date for scheduling has to be valid RFC3339,
  otherwise a parse error will be returned.

Example:

  therm schedules add "2020-01-02T15:04:05Z" "{'queries': ['SELECT * FROM config']}"
  cat queries.json | therm schedules add "2020-01-02T15:04:05Z" -
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *scheduleAddCmd) Synopsis() string {
	return "Add a schedule to be run in the background."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *scheduleAddCmd) Run() clui.ExitCode {
	// Logging.
	var logger log.Logger
	{
		logLevel := level.AllowInfo()
		if c.debug {
			logLevel = level.AllowAll()
		}
		logger = NewLogCluiFormatter(c.UI())
		logger = log.With(logger,
			"ts", log.DefaultTimestampUTC,
			"uid", uuid.NewRandom().String(),
		)
		logger = level.NewFilter(logger, logLevel)
	}

	outputFormat := c.format
	if !contains([]string{"json", "yaml"}, outputFormat) {
		return exit(c.ui, "invalid format type (expected: json|yaml)")
	}

	cmdArgs := c.flagset.Args()
	if len(cmdArgs) != 2 {
		return exit(c.ui, "missing required arguments (expected: 2)")
	}
	date := cmdArgs[0]
	scheduleTime, err := time.Parse(time.RFC3339, date)
	if err != nil || date == "" {
		return exit(c.ui, "invalid date")
	}

	var bits []byte
	if cmdArgs[1] == "-" {
		var err error
		if bits, err = ioutil.ReadAll(os.Stdin); err != nil {
			return exit(c.ui, err.Error())
		}
	} else {
		bits = []byte(cmdArgs[1])
	}

	var queries ScheduleAddQueries
	if err := yaml.Unmarshal(bits, &queries); err != nil {
		return exit(c.ui, err.Error())
	}

	taskQueries := strings.Join(queries.Queries, ";")
	if taskQueries == "" {
		return exit(c.ui, "invalid task queries")
	}

	client, err := getClient(c.address, certs{
		serverCert: c.serverCert,
		clientCert: c.clientCert,
		clientKey:  c.clientKey,
	}, askPassword(c.UI()), logger)
	if err != nil {
		return exit(c.ui, err.Error())
	}

	g := exec.NewGroup()
	exec.Block(g)
	{
		g.Add(func() error {
			data := schedules.ScheduleTaskRequest{
				Query:    taskQueries,
				Schedule: scheduleTime,
			}
			response, _, err := client.Query("POST", "/1.0/schedules", data, "")
			if err != nil {
				return errors.Wrap(err, "error requesting")
			}

			var task schedules.Task
			if err := json.Read(bytes.NewReader(response.Metadata), &task); err != nil {
				return errors.Wrap(err, "error parsting result")
			}

			// just output the whole thing
			content, err := outputContent(outputFormat, task)
			if err != nil {
				return errors.WithStack(err)
			}

			c.ui.Output(string(content))

			return nil
		}, func(err error) {
			// ignore
		})
	}
	exec.Interrupt(g)
	if err := g.Run(); err != nil {
		return exit(c.ui, err.Error())
	}

	return clui.ExitCode{}
}

// ScheduleAddQueries holds the queries for background scheduled tasks
type ScheduleAddQueries struct {
	Queries []string `json:"queries" yaml:"queries"`
}

package main

import (
	"bytes"
	"flag"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/pkg/api/daemon/operations"
)

type operationShowCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	format     string
	serverCert string
	clientCert string
	clientKey  string
}

// NewOperationShowCmd creates a Command with sane defaults
func NewOperationShowCmd(ui clui.UI) clui.Command {
	c := &operationShowCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("version", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *operationShowCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.format, "format", "yaml", "format to output the information json|yaml|tabular")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *operationShowCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *operationShowCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *operationShowCmd) Help() string {
	return `
Usage: 

  operation [flags] show <name>

Description:

  Display a operation
  
  Operation id isn't required in full, partial identifiers
  can be used to show and delete ongoing operations. If
  multiple operations are identified by the partial 
  identifier then a ambiguous error is returned.

Example:

  therm operations show ac025ec4
  therm operations show ac025ec4 --foramt=json
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *operationShowCmd) Synopsis() string {
	return "Show an operation."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *operationShowCmd) Run() clui.ExitCode {
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
	if !contains([]string{"json", "yaml", "tabular"}, outputFormat) {
		return exit(c.ui, "invalid format type (expected: json|yaml|tabular)")
	}

	cmdArgs := c.flagset.Args()
	if len(cmdArgs) != 1 {
		return exit(c.ui, "missing required arguments (expected: 1)")
	}
	id := cmdArgs[0]
	if id == "" {
		return exit(c.ui, "invalid id")
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
			response, _, err := client.Query("GET", fmt.Sprintf("/1.0/operations/%s", id), nil, "")
			if err != nil {
				return errors.Wrap(err, "error requesting")
			}

			var op operations.Op
			if err := json.Read(bytes.NewReader(response.Metadata), &op); err != nil {
				return errors.Wrap(err, "error parsting result")
			}

			// just output the whole thing
			content, err := outputContent(outputFormat, op)
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

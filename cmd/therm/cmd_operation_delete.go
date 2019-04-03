package main

import (
	"flag"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
)

type operationDeleteCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	format     string
	serverCert string
	clientCert string
	clientKey  string
}

// NewOperationDeleteCmd creates a Command with sane defaults
func NewOperationDeleteCmd(ui clui.UI) clui.Command {
	c := &operationDeleteCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("version", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *operationDeleteCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *operationDeleteCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *operationDeleteCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *operationDeleteCmd) Help() string {
	return `
Usage: 

  operation [flags] delete <name>

Description:

  Delete an operation
  
  Operation id isn't required in full, partial identifiers
  can be used to show and delete ongoing operations. If
  multiple operations are identified by the partial 
  identifier then a ambiguous error is returned.

Example:

  therm operations delete ac025ec4
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *operationDeleteCmd) Synopsis() string {
	return "Delete an operation."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *operationDeleteCmd) Run() clui.ExitCode {
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
			response, _, err := client.Query("DELETE", fmt.Sprintf("/1.0/operations/%s", id), nil, "")
			if err != nil {
				return errors.Wrap(err, "error requesting")
			} else if response.StatusCode != 200 {
				return errors.Wrapf(err, "invalid status code %d", response.StatusCode)
			}

			c.ui.Output(fmt.Sprintf("Operation %q deleted", id))

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

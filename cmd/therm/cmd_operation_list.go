package main

import (
	"bytes"
	"flag"

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

type operationListCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	format     string
	serverCert string
	clientCert string
	clientKey  string
}

// NewOperationListCmd creates a Command with sane defaults
func NewOperationListCmd(ui clui.UI) clui.Command {
	c := &operationListCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("version", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *operationListCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.format, "format", "yaml", "format to output the information json|yaml")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *operationListCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *operationListCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *operationListCmd) Help() string {
	return `
Usage: 

  operation [flags] list

Description:

  Operations are asynchronous operations that run in the
  background of a node. If the node goes down, then the
  operation is not guaranteed to be started on another
  cluster node, when it takes over leadership of the
  cluster.
  
  Manual intervention maybe required in the event of a
  operation life cycle termination.
  
  Operation id isn't required in full, partial identifiers
  can be used to show and delete ongoing operations. If
  multiple operations are identified by the partial 
  identifier then a ambiguous error is returned.

Example:

  therm operations list
  therm operations --foramt=json
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *operationListCmd) Synopsis() string {
	return "List available operations."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *operationListCmd) Run() clui.ExitCode {
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
			response, _, err := client.Query("GET", "/1.0/operations", nil, "")
			if err != nil {
				return errors.Wrap(err, "error requesting")
			}

			var ops []operations.Op
			if err := json.Read(bytes.NewReader(response.Metadata), &ops); err != nil {
				return errors.Wrap(err, "error parsting result")
			}

			// just output the whole thing
			content, err := outputContent(outputFormat, ops)
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

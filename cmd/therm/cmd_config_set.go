package main

import (
	"bytes"
	"flag"
	"fmt"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/pkg/api/daemon/root"
)

type configSetCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	format     string
	serverCert string
	clientCert string
	clientKey  string
}

// NewConfigSetCmd creates a Command with sane defaults
func NewConfigSetCmd(ui clui.UI) clui.Command {
	c := &configSetCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("config set", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *configSetCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.format, "format", "yaml", "format to output the information json|yaml|tabular")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *configSetCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *configSetCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *configSetCmd) Help() string {
	return `
Usage: 

  config set [flags] <key> <value>

Description:

  Set a key value of the server configuration

Example:

  therm config set
  therm config set core.https_address [::]:8080
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *configSetCmd) Synopsis() string {
	return "Set a key value of the server configuration."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *configSetCmd) Run() clui.ExitCode {
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
	if a := len(cmdArgs); a < 2 {
		if a == 0 {
			return clui.ExitCode{}
		}
		return exit(c.ui, "missing required arguments")
	}

	var key string
	if len(cmdArgs) > 1 {
		key = cmdArgs[0]
	}
	value := strings.Join(cmdArgs[1:], " ")

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
			response, _, err := client.Query("GET", "/1.0", nil, "")
			if err != nil {
				return errors.Wrap(err, "error requesting")
			} else if response.StatusCode != 200 {
				return errors.Errorf("invalid status code %d", response.StatusCode)
			}

			var server root.Server
			if err := json.Read(bytes.NewReader(response.Metadata), &server); err != nil {
				return errors.Wrap(err, "error parsting result")
			}

			// Config already matches the value, no-op.
			if val, ok := server.Config[key]; ok && val == value {
				return nil
			}

			data := root.ServerUpdate{
				Config: map[string]interface{}{
					key: value,
				},
			}
			response, _, err = client.Query("PATCH", "/1.0", data, "")
			if err != nil {
				return errors.Wrap(err, "error updating")
			} else if response.StatusCode != 200 {
				return errors.Errorf("invalid status code %d", response.StatusCode)
			}

			c.ui.Output(fmt.Sprintf("Set %q to %q", key, value))

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

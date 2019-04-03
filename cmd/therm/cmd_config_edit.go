package main

import (
	libjson "encoding/json"
	"flag"
	"os"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/ask"
	"github.com/spoke-d/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/pkg/api/daemon/root"
	yaml "gopkg.in/yaml.v2"
)

type configEditCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	format     string
	serverCert string
	clientCert string
	clientKey  string
}

// NewConfigEditCmd creates a Command with sane defaults
func NewConfigEditCmd(ui clui.UI) clui.Command {
	c := &configEditCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("config edit", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *configEditCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.format, "format", "yaml", "format to output the information json|yaml|tabular")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *configEditCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *configEditCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *configEditCmd) Help() string {
	return `
Usage: 

  config edit [flags]

Description:

  Edit config for the server configuration

Example:

  therm config edit '{"core.https_address":":8080","core.debug_address":":8081"}'
  therm config edit -
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *configEditCmd) Synopsis() string {
	return "Edit config for the server configuration."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *configEditCmd) Run() clui.ExitCode {
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
	if a := len(cmdArgs); a < 1 {
		if a == 0 {
			return clui.ExitCode{}
		}
		return exit(c.ui, "missing required arguments")
	}

	var err error
	configStr := cmdArgs[0]
	if configStr == "" {
		return exit(c.ui, "invalid config")
	} else if configStr == "-" {
		ask := ask.New(os.Stdin, os.Stdout)
		configStr, err = ask.Text("> ")
		if err != nil {
			return exit(c.ui, "input config")
		}
	} else {
		configStr = strings.Join(cmdArgs[0:], " ")
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
			var config map[string]interface{}
			switch outputFormat {
			case "yaml":
				if err := yaml.Unmarshal([]byte(configStr), &config); err != nil {
					return errors.WithStack(err)
				}
			case "json":
				if err := libjson.Unmarshal([]byte(configStr), &config); err != nil {
					return errors.WithStack(err)
				}
			}
			data := root.ServerUpdate{
				Config: config,
			}
			response, _, err := client.Query("PUT", "/1.0", data, "")
			if err != nil {
				return errors.Wrap(err, "error updating")
			} else if response.StatusCode != 200 {
				return errors.Errorf("invalid status code %d", response.StatusCode)
			}

			c.ui.Output("Configuration edited")

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

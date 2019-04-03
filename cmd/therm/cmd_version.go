package main

import (
	"bytes"
	"flag"
	"runtime"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/internal/version"
	"github.com/spoke-d/thermionic/pkg/api/daemon/root"
)

type versionCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	format     string
	serverCert string
	clientCert string
	clientKey  string
}

// NewVersionCmd creates a Command with sane defaults
func NewVersionCmd(ui clui.UI) clui.Command {
	c := &versionCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("version", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *versionCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.format, "format", "yaml", "format to output the information json|yaml|tabular")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *versionCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *versionCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *versionCmd) Help() string {
	return `
Usage: 

  version [flags]

Description:

  Show client and server version as JSON, YAML or Tabular.

  If the server is unreachable the version output will label the
  'server_version' as "unreachable".

Example:

  therm version
  therm version --foramt=json
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *versionCmd) Synopsis() string {
	return "Show client and server version."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *versionCmd) Run() clui.ExitCode {
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

	client, _ := getClient(c.address, certs{
		serverCert: c.serverCert,
		clientCert: c.clientCert,
		clientKey:  c.clientKey,
	}, askPassword(c.UI()), logger)

	g := exec.NewGroup()
	exec.Block(g)
	{
		g.Add(func() error {
			serverVersion := "unreachable"
			if client != nil {
				response, _, err := client.Query("GET", "/1.0", nil, "")
				if err == nil {
					var server root.Server
					if err := json.Read(bytes.NewReader(response.Metadata), &server); err == nil {
						serverVersion = server.Environment.ServerVersion
					}
				}
			}

			version := struct {
				Client  string `json:"client_version" yaml:"client_version" tab:"client"`
				Server  string `json:"server_version" yaml:"server_version" tab:"server"`
				Runtime string `json:"runtime_version" yaml:"runtime_version" tab:"runtime"`
			}{
				Client:  version.Version,
				Server:  serverVersion,
				Runtime: runtime.Version(),
			}

			// just output the whole thing
			content, err := outputContent(outputFormat, version)
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

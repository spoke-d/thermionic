package main

import (
	"flag"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/pkg/api/daemon/cluster"
)

type clusterRenameCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	format     string
	serverCert string
	clientCert string
	clientKey  string
}

// NewClusterRenameCmd creates a Command with sane defaults
func NewClusterRenameCmd(ui clui.UI) clui.Command {
	c := &clusterRenameCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("cluster", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *clusterRenameCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.format, "format", "yaml", "format to output the information json|yaml|tabular")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *clusterRenameCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *clusterRenameCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *clusterRenameCmd) Help() string {
	return `
Usage: 

  cluster [flags] rename <member> <name>

Description:

  Rename a cluster member.

Example:

  therm cluster rename node1 node2
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *clusterRenameCmd) Synopsis() string {
	return "Rename a cluster member."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *clusterRenameCmd) Run() clui.ExitCode {
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
	member := cmdArgs[0]
	if member == "" {
		return exit(c.ui, "invalid member")
	}
	name := cmdArgs[1]
	if name == "" {
		return exit(c.ui, "invalid name")
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
			enabled, _, err := client.Cluster().Enabled()
			if err != nil {
				return errors.WithStack(err)
			}
			if !enabled {
				return errors.New("instance is not clustered")
			}

			info := cluster.ClusterRenameRequest{
				ServerName: name,
			}
			response, _, err := client.Query("POST", fmt.Sprintf("/1.0/cluster/members/%s", name), info, "")
			if err != nil {
				return errors.Wrap(err, "failed to request members")
			} else if response.StatusCode != 200 {
				return errors.Errorf("invalid status code %d", response.StatusCode)
			}

			c.ui.Output(fmt.Sprintf("Member %q was removed\n", name))

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

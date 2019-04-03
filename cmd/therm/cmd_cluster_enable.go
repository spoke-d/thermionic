package main

import (
	"flag"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
	"github.com/spoke-d/thermionic/client"
	"github.com/spoke-d/thermionic/internal/exec"
)

type clusterEnableCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	serverCert string
	clientCert string
	clientKey  string
}

// NewClusterEnableCmd creates a Command with sane defaults
func NewClusterEnableCmd(ui clui.UI) clui.Command {
	c := &clusterEnableCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("cluster", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *clusterEnableCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *clusterEnableCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *clusterEnableCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *clusterEnableCmd) Help() string {
	return `
Usage: 

  cluster [flags] enable

Description:

  Enable clustering on a single non-clustered instance
  
  This command turns a non-clustered instance into the 
  first member of a new cluster, which will have the 
  given name.
  
  It's required that the node is already available on 
  the network. You can check that by running 
  'therm config get core.https_address', and possibly 
  set a value for the address if not yet set.

Example:

  therm cluster enable
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *clusterEnableCmd) Synopsis() string {
	return "Enable clustering on a single non-clustered instance."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *clusterEnableCmd) Run() clui.ExitCode {
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

	cmdArgs := c.flagset.Args()
	if len(cmdArgs) != 1 {
		return exit(c.ui, "missing required arguments (expected: 1)")
	}
	name := cmdArgs[0]
	if name == "" {
		return exit(c.ui, "invalid name")
	}

	certInfo := client.ClusterCertInfo{}

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
			enabled, etag, err := client.Cluster().Enabled()
			if err != nil {
				return errors.WithStack(err)
			}
			if enabled {
				return errors.New("instance is already clustered")
			}

			server, err := client.Info().Get()
			if err != nil {
				return errors.WithStack(err)
			}

			if addr, ok := server.Config["core.https_address"]; !ok || addr == "" {
				return errors.Wrap(err, "instance is not available on the network")
			}
			if err := client.Cluster().Join(name, "", etag, certInfo); err != nil {
				return errors.Wrap(err, "failed to configure cluster")
			}

			c.ui.Output("Clustering enabled")

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

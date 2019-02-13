package main

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/spoke-d/thermionic/client"
	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
)

type clusterJoinCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	serverCert string
	clientCert string
	clientKey  string
}

// NewClusterJoinCmd creates a Command with sane defaults
func NewClusterJoinCmd(ui clui.UI) clui.Command {
	c := &clusterJoinCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("cluster", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *clusterJoinCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *clusterJoinCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *clusterJoinCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *clusterJoinCmd) Help() string {
	return `
Usage: 

  cluster [flags] join <cluster-address> <cluster-cert>

Description:

  Join an existing cluster, the cluster instance needs
  to be enabled instance.
  
  It's required that the node is already available on 
  the network. You can check that by running 
  'therm config get core.https_address', and possibly 
  set a value for the address if not yet set.

Example:

  cat cluster-config.yaml | therm cluster join 127.0.0.1:8090 node2 -
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *clusterJoinCmd) Synopsis() string {
	return "Join an existing cluster."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *clusterJoinCmd) Run() clui.ExitCode {
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
	if len(cmdArgs) != 3 {
		return exit(c.ui, "missing required arguments (expected: 3)")
	}
	serverName := cmdArgs[0]
	if serverName == "" {
		return exit(c.ui, "invalid server-name")
	}
	clusterAddress := cmdArgs[1]
	if clusterAddress == "" {
		return exit(c.ui, "invalid cluster-address")
	}
	var bits []byte
	if cmdArgs[2] == "-" {
		var err error
		if bits, err = ioutil.ReadAll(os.Stdin); err != nil {
			return exit(c.ui, err.Error())
		}
	} else {
		bits = []byte(cmdArgs[2])
	}

	var config ClusterJoinConfig
	if err := yaml.Unmarshal(bits, &config); err != nil {
		return exit(c.ui, err.Error())
	}

	certInfo := client.ClusterCertInfo{
		Certificate: config.Config["cluster-cert"],
		Key:         config.Config["cluster-key"],
	}
	if certInfo.Certificate == "" || certInfo.Key == "" {
		return exit(c.ui, "invalid cluster credentials")
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
			server, err := client.Info().Get()
			if err != nil {
				return errors.WithStack(err)
			}

			if addr, ok := server.Config["core.https_address"]; !ok || addr == "" {
				return errors.Wrap(err, "instance is not available on the network")
			}
			if err := client.Cluster().Join(serverName, clusterAddress, "", certInfo); err != nil {
				return errors.Wrap(err, "failed to configure cluster")
			}

			c.ui.Output("Joined cluster")

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

// ClusterJoinConfig holds the configuration for joining a cluster
type ClusterJoinConfig struct {
	Config map[string]string `json:"config" yaml:"config"`
}

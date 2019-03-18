package main

import (
	"crypto/x509"
	"flag"
	"net"
	"path/filepath"
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/sys"
	"github.com/spoke-d/thermionic/internal/version"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/spoke-d/thermionic/pkg/api/discovery/root"
	"github.com/spoke-d/thermionic/pkg/api/discovery/shutdown"
	"github.com/spoke-d/thermionic/pkg/api/discovery/waitready"
	"github.com/spoke-d/thermionic/pkg/discovery"
)

type discoverInitCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	networkAddress string
	networkPort    int
	debugAddress   string
	debugPort      int

	debug      bool
	serverCert string
	clientCert string
	clientKey  string

	bindAddress      string
	advertiseAddress string
}

// NewDiscoverInitCmd creates a Command with sane defaults
func NewDiscoverInitCmd(ui clui.UI) clui.Command {
	c := &discoverInitCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("discover", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *discoverInitCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.networkAddress, "network-address", "127.0.0.1", "address to bind to")
	c.flagset.IntVar(&c.networkPort, "network-port", 8082, "port to bind to")
	c.flagset.StringVar(&c.debugAddress, "debug-address", "", "debug address to bind to")
	c.flagset.IntVar(&c.debugPort, "debug-port", 8083, "debug port to bind to")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
	c.flagset.StringVar(&c.bindAddress, "bind-address", "127.0.0.1:8085", "bind address")
	c.flagset.StringVar(&c.advertiseAddress, "advertise-address", "127.0.0.1:8086", "advertise address")
}

// UI returns a UI for interaction.
func (c *discoverInitCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *discoverInitCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *discoverInitCmd) Help() string {
	return `
Usage: 

  discover init [flags] <leader-daemon-address> <leader-daemon-nonce> <daemon-address> <daemon-nonce>

Description:

  Discover other daemons on the same network, to
  allow automatic management of clustering.

  The discovery agent requires access to both the
  leader and subordinate daemon.

Example:

  cat nonce.txt nonce2.txt | xargs sh -c 'therm discover init 127.0.0.1:9080 $0 127.0.0.1:8080 $1'
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *discoverInitCmd) Synopsis() string {
	return "Automate discovery management."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *discoverInitCmd) Run() clui.ExitCode {
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

	var fileSystem fsys.FileSystem
	{
		config, err := fsys.Build(
			fsys.With("local"),
		)
		if err != nil {
			return exit(c.ui, err.Error())
		}
		fs, err := fsys.New(config)
		if err != nil {
			return exit(c.ui, err.Error())
		}
		fileSystem = fs
	}

	cmdArgs := c.flagset.Args()
	if len(cmdArgs) != 4 {
		return exit(c.ui, "missing required arguments (expected: 4)")
	}
	leaderDaemonAddress := cmdArgs[0]
	if leaderDaemonAddress == "" {
		return exit(c.ui, "invalid leader daemon address")
	}
	leaderDaemonNonce := cmdArgs[1]
	if leaderDaemonNonce == "" {
		return exit(c.ui, "invalid leader daemon nonce")
	}
	daemonAddress := cmdArgs[2]
	if daemonAddress == "" {
		return exit(c.ui, "invalid daemon address")
	}
	daemonNonce := cmdArgs[3]
	if daemonNonce == "" {
		return exit(c.ui, "invalid daemon nonce")
	}

	os := sys.DefaultOS()
	if err := os.Init(fileSystem); err != nil {
		return exit(c.ui, err.Error())
	}

	bindHost, bindPort, err := parseDiscoveryAddr(c.bindAddress, defaultBindPort)
	if err != nil {
		return exit(c.ui, err.Error())
	}
	advertiseHost, advertisePort, err := parseDiscoveryAddr(c.advertiseAddress, defaultAdvertisePort)
	if err != nil {
		return exit(c.ui, err.Error())
	}

	// daemon client used to gather information required to start the discovery
	// service.
	client, err := getClient(daemonAddress, certs{
		serverCert: c.serverCert,
		clientCert: c.clientCert,
		clientKey:  c.clientKey,
	}, askPassword(c.UI()), logger)
	if err != nil {
		return exit(c.ui, err.Error())
	}
	daemonInfo, err := client.Info().Get()
	if err != nil {
		return exit(c.ui, err.Error())
	}

	configValues := make(map[string]string)
	for k, v := range daemonInfo.Config {
		// check we should add the config to the cluster configuration
		if _, ok := clusterconfig.Schema[k]; !ok {
			continue
		}
		if s, ok := v.(string); ok {
			configValues[k] = s
		}
	}
	config, err := clusterconfig.NewReadOnlyConfig(configValues, clusterconfig.Schema)
	if err != nil {
		return exit(c.ui, err.Error())
	}

	clientCerts, err := cert.ReadCert(
		filepath.Join(os.VarDir(), "client.crt"),
		cert.WithFileSystem(fileSystem),
	)
	if err != nil {
		return exit(c.ui, err.Error())
	}
	certInfo, err := cert.LoadCert(
		os.VarDir(),
		cert.WithFileSystem(fileSystem),
	)
	if err != nil {
		return exit(c.ui, err.Error())
	}
	serverCerts, err := cert.ReadCert(
		filepath.Join(os.VarDir(), "server.crt"),
		cert.WithFileSystem(fileSystem),
	)
	if err != nil {
		return exit(c.ui, err.Error())
	}

	// register the API services
	apiServices := []api.Service{
		root.NewAPI(config),
	}
	apiInternalServices := []api.Service{
		waitready.NewAPI("ready"),
		shutdown.NewAPI("shutdown"),
	}
	service := discovery.NewService(
		certInfo,
		[]x509.Certificate{*clientCerts},
		[]x509.Certificate{*serverCerts},
		config,
		version.Version,
		net.JoinHostPort(c.networkAddress, strconv.Itoa(c.networkPort)),
		net.JoinHostPort(c.debugAddress, strconv.Itoa(c.debugPort)),
		apiServices,
		apiInternalServices,
		leaderDaemonAddress, leaderDaemonNonce,
		discovery.WithLogger(log.With(logger, "component", "discovery")),
	)

	var cancel <-chan struct{}
	g := exec.NewGroup()
	exec.Block(g)
	{
		g.Add(func() error {
			c.ui.Output("Starting Discovery Service.")
			if err := service.Init(
				discovery.WithDaemon(daemonAddress, daemonNonce),
				discovery.WithBindAddrPort(bindHost, bindPort),
				discovery.WithAdvertiseAddrPort(advertiseHost, advertisePort),
			); err != nil {
				return errors.WithStack(err)
			}
			select {
			case <-cancel:
				level.Info(logger).Log("msg", "Received signal exiting")
			case <-service.ShutdownChan():
				level.Debug(logger).Log("msg", "Shutting down API")
			}
			return service.Stop()
		}, func(err error) {
			service.Kill()
		})
	}
	cancel = exec.Interrupt(g)
	if err := g.Run(); err != nil {
		return exit(c.ui, err.Error())
	}

	return clui.ExitCode{}
}

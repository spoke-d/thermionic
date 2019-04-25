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
	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
	"github.com/spoke-d/thermionic/internal/actors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/sys"
	"github.com/spoke-d/thermionic/internal/version"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/spoke-d/thermionic/pkg/api/daemon/cluster"
	"github.com/spoke-d/thermionic/pkg/api/daemon/events"
	"github.com/spoke-d/thermionic/pkg/api/daemon/operations"
	"github.com/spoke-d/thermionic/pkg/api/daemon/query"
	"github.com/spoke-d/thermionic/pkg/api/daemon/root"
	"github.com/spoke-d/thermionic/pkg/api/daemon/schedules"
	"github.com/spoke-d/thermionic/pkg/api/daemon/services"
	"github.com/spoke-d/thermionic/pkg/api/daemon/shutdown"
	"github.com/spoke-d/thermionic/pkg/api/daemon/waitready"
	"github.com/spoke-d/thermionic/pkg/daemon"
	evts "github.com/spoke-d/thermionic/pkg/events"
)

type daemonInitCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug          bool
	discoverable   bool
	format         string
	networkAddress string
	networkPort    int
	debugAddress   string
	debugPort      int
}

// NewDaemonInitCmd creates a Command with sane defaults
func NewDaemonInitCmd(ui clui.UI) clui.Command {
	c := &daemonInitCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("version", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *daemonInitCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.BoolVar(&c.discoverable, "discoverable", false, "discovery service enabled")
	c.flagset.StringVar(&c.format, "format", "yaml", "format to output the information json|yaml|tabular")
	c.flagset.StringVar(&c.networkAddress, "network-address", "127.0.0.1", "address to bind to")
	c.flagset.IntVar(&c.networkPort, "network-port", 8080, "port to bind to")
	c.flagset.StringVar(&c.debugAddress, "debug-address", "", "debug address to bind to")
	c.flagset.IntVar(&c.debugPort, "debug-port", 8081, "debug port to bind to")
}

// UI returns a UI for interaction.
func (c *daemonInitCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *daemonInitCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *daemonInitCmd) Help() string {
	return `
Usage: 

  daemon [flags]

Description:

  The therm API manager (daemon)

  This is the therm daemon command line. The daemon is 
  typically started directly by your system and interacted 
  with through the rest API or other subcommands. Those 
  subcommands allow you to interact directly with the local
  daemon.

Example:

  therm daemon init
  therm daemon init --network-port=8080
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *daemonInitCmd) Synopsis() string {
	return "Initialise Daemon service."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *daemonInitCmd) Run() clui.ExitCode {
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

	os := sys.DefaultOS()
	if err := os.Init(fileSystem); err != nil {
		return exit(c.ui, err.Error())
	}

	_, _, err := readOrGenerateClientCertificates(fileSystem, os.VarDir(), askPassword(c.UI()))
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

	// setup the operation collection
	actorGroup := actors.NewGroup()
	eventBroadcaster := evts.NewEventBroadcaster(
		makeEventsActorGroupShim(actorGroup),
		log.WithPrefix(logger, "component", "event-broadcaster"),
	)

	// register the API services
	apiServices := []api.Service{
		root.NewAPI(node.ConfigSchema),
		events.NewAPI("events"),
		cluster.NewAPI(
			"cluster",
			cluster.WithLogger(log.WithPrefix(logger, "api", "cluster")),
			cluster.WithFileSystem(fileSystem),
		),
		cluster.NewMembersAPI(
			"cluster/members",
			cluster.WithLogger(log.WithPrefix(logger, "api", "cluster")),
			cluster.WithFileSystem(fileSystem),
		),
		cluster.NewMemberNodesAPI(
			"cluster/members/{name}",
			cluster.WithLogger(log.WithPrefix(logger, "api", "cluster")),
			cluster.WithFileSystem(fileSystem),
		),
		operations.NewAPI(
			"operations",
			operations.WithLogger(log.WithPrefix(logger, "api", "operations")),
		),
		operations.NewIdentityAPI(
			"operations/{id}",
			operations.WithLogger(log.WithPrefix(logger, "api", "identity-operations")),
		),
		operations.NewWaitAPI(
			"operations/{id}/wait",
			operations.WithLogger(log.WithPrefix(logger, "api", "wait-operations")),
		),
		services.NewAPI(
			"services",
			services.WithLogger(log.WithPrefix(logger, "api", "services")),
			services.WithFileSystem(fileSystem),
		),
		services.NewMembersAPI(
			"services/members",
			services.WithLogger(log.WithPrefix(logger, "api", "services")),
			services.WithFileSystem(fileSystem),
		),
		services.NewMemberNodesAPI(
			"services/members/{name}",
			services.WithLogger(log.WithPrefix(logger, "api", "services")),
			services.WithFileSystem(fileSystem),
		),
		schedules.NewAPI(
			"schedules",
			schedules.WithLogger(log.WithPrefix(logger, "api", "schedules")),
		),
		schedules.NewIdentityAPI(
			"schedules/{id}",
			schedules.WithLogger(log.WithPrefix(logger, "api", "identity-schedules")),
		),
	}
	apiInternalServices := []api.Service{
		query.NewAPI("query"),
		cluster.NewAcceptAPI(
			"cluster/accept",
			cluster.WithLogger(log.WithPrefix(logger, "api", "cluster")),
			cluster.WithFileSystem(fileSystem),
		),
		cluster.NewRebalanceAPI(
			"cluster/rebalance",
			cluster.WithLogger(log.WithPrefix(logger, "api", "cluster")),
			cluster.WithFileSystem(fileSystem),
		),
		cluster.NewPromoteAPI(
			"cluster/promote",
			cluster.WithLogger(log.WithPrefix(logger, "api", "cluster")),
			cluster.WithFileSystem(fileSystem),
		),
		waitready.NewAPI("ready"),
		shutdown.NewAPI("shutdown"),
	}

	daemon := daemon.New(
		[]x509.Certificate{
			*clientCerts,
		},
		version.Version,
		net.JoinHostPort(c.networkAddress, strconv.Itoa(c.networkPort)),
		net.JoinHostPort(c.debugAddress, strconv.Itoa(c.debugPort)),
		config.Schema,
		node.ConfigSchema,
		APIExtensions,
		apiServices,
		apiInternalServices,
		makeActorGroupShim(actorGroup),
		eventBroadcaster,
		daemon.WithOS(sys.DefaultOS()),
		daemon.WithFileSystem(fileSystem),
		daemon.WithLogger(log.WithPrefix(logger, "component", "daemon")),
		daemon.WithDiscoverable(c.discoverable),
	)

	var cancel <-chan struct{}
	g := exec.NewGroup()
	exec.Block(g)
	{
		g.Add(func() error {
			c.ui.Output("Starting Daemon.")
			if err := daemon.Init(); err != nil {
				return errors.WithStack(err)
			}
			select {
			case <-cancel:
				level.Info(logger).Log("msg", "Received signal exiting")
			case <-daemon.ShutdownChan():
				level.Debug(logger).Log("msg", "Shutting down API")
			}
			return daemon.Stop()
		}, func(err error) {
			daemon.Kill()
		})
	}
	cancel = exec.Interrupt(g)
	if err := g.Run(); err != nil {
		return exit(c.ui, err.Error())
	}

	return clui.ExitCode{}
}

type actorGroupShim struct {
	actorGroup *actors.Group
}

func makeActorGroupShim(actorGroup *actors.Group) actorGroupShim {
	return actorGroupShim{
		actorGroup: actorGroup,
	}
}

func (s actorGroupShim) Add(a daemon.Actor) {
	s.actorGroup.Add(a)
}

func (s actorGroupShim) Prune() bool {
	return s.actorGroup.Prune()
}

func (s actorGroupShim) Walk(fn func(daemon.Actor) error) error {
	return s.actorGroup.Walk(func(a actors.Actor) error {
		return fn(a)
	})
}

type eventsActorGroupShim struct {
	actorGroup *actors.Group
}

func makeEventsActorGroupShim(actorGroup *actors.Group) eventsActorGroupShim {
	return eventsActorGroupShim{
		actorGroup: actorGroup,
	}
}

func (s eventsActorGroupShim) Add(a evts.Actor) {
	s.actorGroup.Add(a)
}

func (s eventsActorGroupShim) Prune() bool {
	return s.actorGroup.Prune()
}

func (s eventsActorGroupShim) Walk(fn func(evts.Actor) error) error {
	return s.actorGroup.Walk(func(a actors.Actor) error {
		return fn(a)
	})
}

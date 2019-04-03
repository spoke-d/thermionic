package main

import (
	"flag"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/pkg/events"
)

type daemonShutdownCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	serverCert string
	clientCert string
	clientKey  string
	timeout    string
}

// NewDaemonShutdownCmd creates a Command with sane defaults
func NewDaemonShutdownCmd(ui clui.UI) clui.Command {
	c := &daemonShutdownCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("shutdown", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *daemonShutdownCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
	c.flagset.StringVar(&c.timeout, "timeout", "0", "duration of time to wait before giving up")
}

// UI returns a UI for interaction.
func (c *daemonShutdownCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *daemonShutdownCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *daemonShutdownCmd) Help() string {
	return `
Usage: 

  shutdown [flags]

Description:

  Tell the daemon to shutdown.
  
  This will tell the daemon to start cleaning up any background
  processes, followed by having itself shutdown and exit.

Example:

  therm daemon shutdown
  therm daemon shutdown --timeout=1m
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *daemonShutdownCmd) Synopsis() string {
	return "Shutdown all services and exit."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *daemonShutdownCmd) Run() clui.ExitCode {
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

	timeoutDuration, err := time.ParseDuration(c.timeout)
	if err != nil {
		return exit(c.ui, err.Error())
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
		done := make(chan error, 1)
		g.Add(func() error {
			response, _, err := client.Query("GET", "/internal/shutdown", nil, "")
			if err != nil {
				return err
			} else if response.StatusCode != 200 {
				return errors.Errorf("invalid response status code: %d", response.StatusCode)
			}

			go func() {
				eventTask := events.NewEvents(client.RawClient())
				listener, err := eventTask.GetEvents()
				if err != nil {
					close(done)
					return
				}

				listener.Wait()
				close(done)
			}()

			if timeoutDuration > 0 {
				select {
				case <-done:
					break
				case <-time.After(timeoutDuration):
					return errors.Errorf("Daemon is still running after %v timeout", timeoutDuration)
				}
			} else {
				select {
				case <-done:
					break
				}
			}
			c.ui.Output("Daemon shutdown")

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

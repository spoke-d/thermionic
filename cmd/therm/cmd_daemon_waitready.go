package main

import (
	"flag"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
)

type daemonWaitReadyCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	timeout    string
	address    string
	serverCert string
	clientCert string
	clientKey  string
}

// NewDaemonWaitReadyCmd creates a Command with sane defaults
func NewDaemonWaitReadyCmd(ui clui.UI) clui.Command {
	c := &daemonWaitReadyCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("waitready", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *daemonWaitReadyCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.timeout, "timeout", "0", "duration of time to wait before giving up")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *daemonWaitReadyCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *daemonWaitReadyCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *daemonWaitReadyCmd) Help() string {
	return `
Usage: 

  waitready [flags]

Description:

  Wait for the daemon to be ready to process requests.

  This command will block until the daemon is reachable
  over its REST API and is done with early start tasks
  like event scheduling.

Example:

  therm daemon waitready
  therm daemon waitready --timeout=1m
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *daemonWaitReadyCmd) Synopsis() string {
	return "Wait for the daemon to be ready to process requests."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *daemonWaitReadyCmd) Run() clui.ExitCode {
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

	g := exec.NewGroup()
	exec.Block(g)
	{
		done := make(chan error, 1)
		g.Add(func() error {
			var errLast error
			go func() {
				for i := 0; ; i++ {

					var output bool
					if i > 10 {
						output = i < 30 || ((i % 10) == 0)
					}
					if output {
						level.Debug(logger).Log("msg", "Connecting to daemon", "attempt", i)
					}

					client, err := getClient(c.address, certs{
						serverCert: c.serverCert,
						clientCert: c.clientCert,
						clientKey:  c.clientKey,
					}, askPassword(c.UI()), logger)
					if err != nil {
						errLast = err
						if output {
							level.Debug(logger).Log("msg", "failed to check daemon", "attempt", i, "err", errLast)
						}
						time.Sleep(500 * time.Millisecond)
						continue
					}

					var failure bool
					response, _, err := client.Query("GET", "/internal/ready", nil, "")
					if err != nil {
						errLast = err
						failure = true
					} else if response.StatusCode != 200 {
						errLast = errors.Errorf("invalid response status code: %d", response.StatusCode)
						failure = true
					}
					if failure {
						if output {
							level.Debug(logger).Log("msg", "failed to check daemon", "attempt", i, "err", errLast)
						}
						time.Sleep(500 * time.Millisecond)
						continue
					}

					done <- nil
					return
				}
			}()

			if timeoutDuration > 0 {
				select {
				case <-done:
					break
				case <-time.After(timeoutDuration):
					return errors.Errorf("Daemon is not running after %v timeout (%v)", timeoutDuration, errLast)
				}
			} else {
				select {
				case <-done:
					break
				}
			}
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

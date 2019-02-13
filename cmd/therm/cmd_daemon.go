package main

import (
	"flag"

	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
)

type daemonCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet
}

// NewDaemonCmd creates a Command with sane defaults
func NewDaemonCmd(ui clui.UI) clui.Command {
	c := &daemonCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("daemon", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *daemonCmd) init() {
}

// UI returns a UI for interaction.
func (c *daemonCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *daemonCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *daemonCmd) Help() string {
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
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *daemonCmd) Synopsis() string {
	return "Daemon service."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *daemonCmd) Run() clui.ExitCode {
	return clui.ExitCode{
		ShowHelp: true,
	}
}

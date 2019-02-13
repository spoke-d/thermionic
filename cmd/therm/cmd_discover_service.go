package main

import (
	"flag"

	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
)

type discoverServiceCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet
}

// NewDiscoverServiceCmd creates a Command with sane defaults
func NewDiscoverServiceCmd(ui clui.UI) clui.Command {
	c := &discoverServiceCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("discover", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *discoverServiceCmd) init() {
}

// UI returns a UI for interaction.
func (c *discoverServiceCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *discoverServiceCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *discoverServiceCmd) Help() string {
	return `
Usage: 

  discover service [flags]

Description:

  Discover other services on the same network, to
  allow automatic management of clustering.
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *discoverServiceCmd) Synopsis() string {
	return "Manage discovery service management."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *discoverServiceCmd) Run() clui.ExitCode {
	return clui.ExitCode{
		ShowHelp: true,
	}
}

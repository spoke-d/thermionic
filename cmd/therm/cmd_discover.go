package main

import (
	"flag"

	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
)

const (
	defaultClientPort    = 8082
	defaultBindPort      = 8083
	defaultAdvertisePort = 8084
)

type discoverCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet
}

// NewDiscoverCmd creates a Command with sane defaults
func NewDiscoverCmd(ui clui.UI) clui.Command {
	c := &discoverCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("discover", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *discoverCmd) init() {
}

// UI returns a UI for interaction.
func (c *discoverCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *discoverCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *discoverCmd) Help() string {
	return `
Usage: 

  discover [flags]

Description:

  Discover other daemons on the same network, to
  allow automatic management of clustering.
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *discoverCmd) Synopsis() string {
	return "Manage discovery management."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *discoverCmd) Run() clui.ExitCode {
	return clui.ExitCode{
		ShowHelp: true,
	}
}

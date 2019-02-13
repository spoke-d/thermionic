package main

import (
	"flag"

	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
)

type clusterCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet
}

// NewClusterCmd creates a Command with sane defaults
func NewClusterCmd(ui clui.UI) clui.Command {
	c := &clusterCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("cluster", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *clusterCmd) init() {
}

// UI returns a UI for interaction.
func (c *clusterCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *clusterCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *clusterCmd) Help() string {
	return `
Usage: 

  cluster [flags]

Description:

  Manage clusters.
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *clusterCmd) Synopsis() string {
	return "Manage clusters."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *clusterCmd) Run() clui.ExitCode {
	return clui.ExitCode{
		ShowHelp: true,
	}
}

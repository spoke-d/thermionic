package main

import (
	"flag"

	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
)

type configCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug          bool
	format         string
	networkAddress string
	networkPort    int
	debugAddress   string
	debugPort      int
}

// NewConfigCmd creates a Command with sane defaults
func NewConfigCmd(ui clui.UI) clui.Command {
	c := &configCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("config", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *configCmd) init() {
}

// UI returns a UI for interaction.
func (c *configCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *configCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *configCmd) Help() string {
	return `
Usage: 

  config [flags]

Description:

  Manage configuration settings of both the local 
  and remote cluster data sources.
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *configCmd) Synopsis() string {
	return "Manage configuration."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *configCmd) Run() clui.ExitCode {
	return clui.ExitCode{
		ShowHelp: true,
	}
}

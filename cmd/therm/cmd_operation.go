package main

import (
	"flag"

	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
)

type operationCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet
}

// NewOperationCmd creates a Command with sane defaults
func NewOperationCmd(ui clui.UI) clui.Command {
	c := &operationCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("operation", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *operationCmd) init() {
}

// UI returns a UI for interaction.
func (c *operationCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *operationCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *operationCmd) Help() string {
	return `
Usage: 

  operation [flags]

Description:

  Manage operations that should be run on the
  underlying cluster database at a given time.
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *operationCmd) Synopsis() string {
	return "Manage operations."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *operationCmd) Run() clui.ExitCode {
	return clui.ExitCode{
		ShowHelp: true,
	}
}

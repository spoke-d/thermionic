package main

import (
	"flag"

	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
)

type scheduleCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet
}

// NewScheduleCmd creates a Command with sane defaults
func NewScheduleCmd(ui clui.UI) clui.Command {
	c := &scheduleCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("schedule", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *scheduleCmd) init() {
}

// UI returns a UI for interaction.
func (c *scheduleCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *scheduleCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *scheduleCmd) Help() string {
	return `
Usage: 

  schedule [flags]

Description:

  Manage scheduled tasks that should be run on the
  underlying cluster database at a given time.
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *scheduleCmd) Synopsis() string {
	return "Manage scheduled tasks."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *scheduleCmd) Run() clui.ExitCode {
	return clui.ExitCode{
		ShowHelp: true,
	}
}

package main

import (
	"fmt"
	"os"

	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/style"
)

const header = `
███████╗██████╗  ██████╗ ██╗  ██╗███████╗   ██████╗ 
██╔════╝██╔══██╗██╔═══██╗██║ ██╔╝██╔════╝   ██╔══██╗
███████╗██████╔╝██║   ██║█████╔╝ █████╗     ██║  ██║
╚════██║██╔═══╝ ██║   ██║██╔═██╗ ██╔══╝     ██║  ██║
███████║██║     ╚██████╔╝██║  ██╗███████╗██╗██████╔╝
╚══════╝╚═╝      ╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝╚═════╝ 
`

const padding = 2

func main() {
	ui := clui.NewColorUI(clui.NewBasicUI(os.Stdin, os.Stdout))
	ui.OutputColor = style.New(style.FgWhite)
	ui.InfoColor = style.New(style.FgGreen)
	ui.WarnColor = style.New(style.FgYellow)
	ui.ErrorColor = style.New(style.FgRed)

	cli := clui.NewCLI("therm", "0.0.1", header, clui.CLIOptions{
		UI: ui,
	})
	cli.AddCommand("cluster", NewClusterCmd(ui))
	cli.AddCommand("cluster enable", NewClusterEnableCmd(ui))
	cli.AddCommand("cluster join", NewClusterJoinCmd(ui))
	cli.AddCommand("cluster list", NewClusterListCmd(ui))
	cli.AddCommand("cluster remove", NewClusterRemoveCmd(ui))
	cli.AddCommand("cluster rename", NewClusterRenameCmd(ui))
	cli.AddCommand("cluster show", NewClusterShowCmd(ui))
	cli.AddCommand("config", NewConfigCmd(ui))
	cli.AddCommand("config edit", NewConfigEditCmd(ui))
	cli.AddCommand("config get", NewConfigGetCmd(ui))
	cli.AddCommand("config set", NewConfigSetCmd(ui))
	cli.AddCommand("config show", NewConfigShowCmd(ui))
	cli.AddCommand("daemon", NewDaemonCmd(ui))
	cli.AddCommand("daemon init", NewDaemonInitCmd(ui))
	cli.AddCommand("daemon shutdown", NewDaemonShutdownCmd(ui))
	cli.AddCommand("daemon waitready", NewDaemonWaitReadyCmd(ui))
	cli.AddCommand("discover", NewDiscoverCmd(ui))
	cli.AddCommand("discover init", NewDiscoverInitCmd(ui))
	cli.AddCommand("discover shutdown", NewDiscoverShutdownCmd(ui))
	cli.AddCommand("discover waitready", NewDiscoverWaitReadyCmd(ui))
	cli.AddCommand("discover service", NewDiscoverServiceCmd(ui))
	cli.AddCommand("discover service delete", NewDiscoverServiceDeleteCmd(ui))
	cli.AddCommand("discover service list", NewDiscoverServiceListCmd(ui))
	cli.AddCommand("info", NewInfoCmd(ui))
	cli.AddCommand("operation", NewOperationCmd(ui))
	cli.AddCommand("operation delete", NewOperationDeleteCmd(ui))
	cli.AddCommand("operation show", NewOperationShowCmd(ui))
	cli.AddCommand("operation list", NewOperationListCmd(ui))
	cli.AddCommand("query", NewQueryCmd(ui))
	cli.AddCommand("schedule", NewScheduleCmd(ui))
	cli.AddCommand("schedule add", NewScheduleAddCmd(ui))
	cli.AddCommand("schedule delete", NewScheduleDeleteCmd(ui))
	cli.AddCommand("schedule list", NewScheduleListCmd(ui))
	cli.AddCommand("schedule show", NewScheduleShowCmd(ui))
	cli.AddCommand("version", NewVersionCmd(ui))

	exitCode, err := cli.Run(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(exitCode.Code())
}

func exit(ui clui.UI, err string) clui.ExitCode {
	ui.Error(err)
	return clui.ExitCode{
		Code: clui.EPerm,
	}
}

// APIExtensions is the list of all API extensions in the order they were added.
var APIExtensions = []string{
	"etag",
	"clustering",
	"schedule-tasks",
	"discovery",
}

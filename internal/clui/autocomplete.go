package clui

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/spoke-d/thermionic/internal/clui/args"
	"github.com/spoke-d/thermionic/internal/clui/radix"
)

const (
	envComplete = "COMP_LINE"
	envDebug    = "COMP_DEBUG"
)

// AutoCompleteInstaller is an interface to be implemented to perform the
// autocomplete installation and uninstallation with a CLI.
//
// This interface is not exported because it only exists for unit tests
// to be able to test that the installation is called properly.
type AutoCompleteInstaller interface {
	Install(string) error
	Uninstall(string) error
}

// AutoComplete defines a way to predict and complete arguments passed
// in to the CLI
type AutoComplete struct {
	ui        UI
	installer AutoCompleteInstaller
	registry  *Registry
}

// NewAutoComplete creates a new AutoComplete with the correct dependencies.
func NewAutoComplete(ui UI, installer AutoCompleteInstaller, registry *Registry) *AutoComplete {
	return &AutoComplete{
		ui:        ui,
		installer: installer,
		registry:  registry,
	}
}

// Install a command into the host using the AutoCompleteInstaller.
// Returns an error if there is an error whilst installing.
func (a *AutoComplete) Install(cmd string) error {
	return a.installer.Install(cmd)
}

// Uninstall the command from the host using the AutoCompleteInstaller.
// Returns an error if there is an error whilst it's uninstalling.
func (a *AutoComplete) Uninstall(cmd string) error {
	return a.installer.Uninstall(cmd)
}

// Complete a command from completion line in environment variable,
// and print out the complete options.
// returns success if the completion ran or if the cli matched
// any of the given flags, false otherwise
func (a *AutoComplete) Complete() bool {
	line, ok := getLine()
	if !ok {
		return false
	}

	var (
		matches []string

		args    = args.New(line)
		options = a.Predict(args)
	)

	for _, opt := range options {
		if strings.HasPrefix(opt, args.Last) {
			matches = append(matches, opt)
		}
	}

	a.output(matches)
	return true
}

// Predict returns all possible predictions for args according to the command struct
func (a *AutoComplete) Predict(v *args.Args) (options []string) {
	var (
		potential []pair
		args      = strings.Join(v.AllCommands(), " ")
	)
	a.registry.commandTree.WalkPrefix(args, func(s string, cmd radix.Value) bool {
		if c, ok := cmd.(Command); ok {
			potential = append(potential, pair{
				Name:    s,
				Command: c,
			})
		}
		return false
	})

	if len(potential) > 0 {
		if isFlag := strings.HasPrefix(v.Last, "-"); isFlag {
			// Check if the potential cmd is an exact match
			var cmds []pair
			for _, pair := range potential {
				if strings.HasSuffix(pair.Name, v.LastCompleted) {
					cmds = append(cmds, pair)
				}
			}

			// find out what those flags are
			for _, pair := range cmds {
				opts, only := predictFlag(pair.Command, v)
				options = append(options, opts...)
				if only {
					return
				}
			}
		} else {
			// auto complete the command name
			for _, pair := range potential {
				parts := strings.Split(pair.Name, " ")
				if len(parts) > 1 {
					options = append(options, parts[len(parts)-1])
				}
			}
		}
	} else {
		// TODO: auto complete files
	}
	return
}

func (a *AutoComplete) output(options []string) {
	// stdout of program defines the complete options
	for _, option := range options {
		a.ui.Output(option)
	}
}

func getLine() (string, bool) {
	line := os.Getenv(envComplete)
	if line == "" {
		return "", false
	}
	return line, true
}

type pair struct {
	Name    string
	Command Command
}

func predictFlag(cmd Command, a *args.Args) (options []string, only bool) {
	flagset := cmd.FlagSet()

	flagset.VisitAll(func(f *flag.Flag) {
		options = append(options, fmt.Sprintf("-%s", f.Name))
	})

	if flag := flagset.Lookup(a.LastCompleted); flag != nil {
		return append(options, flag.Name), true
	}

	return
}

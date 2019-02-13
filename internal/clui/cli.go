package clui

import (
	"bytes"
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spoke-d/thermionic/internal/clui/install"
	"github.com/spoke-d/thermionic/internal/clui/radix"
	"github.com/pkg/errors"
)

// Errno represents a error constants that can be reutrned from the CLI
type Errno int

// Code converts the Errno back into an int when type inference fails.
func (e Errno) Code() int {
	return int(e)
}

const (
	// EOK is non-standard representation of a success
	EOK Errno = 0

	// EPerm represents an operation not permitted
	EPerm Errno = 1

	// EKeyExpired is outside of POSIX 1, represents unknown error.
	EKeyExpired Errno = 127
)

// CLIOptions allows the changing of CLI options
type CLIOptions struct {
	// HelpFunc is the function called to generate the generic help text that is
	// shown. If help must be shown for the CLI that doesn't pertain to a
	// specific command.
	HelpFunc HelpFunc

	// Installer
	Installer AutoCompleteInstaller

	UI UI
}

// Build a CLI from the CLIOptions
func (c CLIOptions) Build(cli *CLI) {
	if c.HelpFunc != nil {
		cli.helpFunc = c.HelpFunc
	}
	if c.Installer != nil {
		cli.autoComplete.installer = c.Installer
	}
	if c.UI != nil {
		cli.ui = c.UI
		cli.autoComplete.ui = c.UI
	}
}

// CLI contains the state necessary to run commands and parse the command line
// arguments
//
// CLI also supports nested subCommands, such as "cli foo bar". To use nested
// subCommands, the key in the Commands mapping below contains the full
// subCommand.
// In this example, it would be "foo bar"
type CLI struct {

	// Name defines the name of the CLI
	name string

	// Version of the CLI
	version string

	// Header to output
	header string

	// Default ui
	ui UI

	// HelpFunc and HelpWriter are used to output help information, if
	// requested.
	//
	// HelpFunc is the function called to generate the generic help text that is
	// shown. If help must be shown for the CLI that doesn't pertain to a
	// specific command.
	//
	// HelpWriter is the Writer where the help text is outputted to. If not
	// specified, it will default to Stderr.
	helpFunc HelpFunc

	commands     *Registry
	commandFlags []string

	subCommand     string
	subCommandArgs []string

	// AutoComplete
	autoComplete *AutoComplete

	// These are special global flags
	isHelp, isVersion                              bool
	isAutoCompleteInstall, isAutoCompleteUninstall bool
}

// NewCLI returns a new CLI instance with sensible default.
func NewCLI(name, version, header string, options ...CLIOptions) *CLI {
	registry := NewRegistry()
	cli := &CLI{
		name:     name,
		version:  version,
		header:   header,
		ui:       NewNopUI(),
		helpFunc: BasicHelpFunc(name),
		commands: registry,
		autoComplete: NewAutoComplete(
			NewNopUI(),
			install.NewNop(),
			registry,
		),
	}

	for _, v := range options {
		v.Build(cli)
	}

	return cli
}

// AddCommand inserts a new command to the CLI.
func (c *CLI) AddCommand(key string, cmd Command) error {
	return c.commands.Add(key, cmd)
}

// Run runs the actual CLI bases on the arguments given.
func (c *CLI) Run(args []string) (Errno, error) {
	if err := c.commands.Process(); err != nil {
		return EPerm, err
	}

	if err := c.processArgs(args); err != nil {
		return EPerm, err
	}

	if c.isAutoCompleteInstall && c.isAutoCompleteUninstall {
		return EPerm, fmt.Errorf("both autocomplete flags can not be used at the same time")
	}

	// If this is a autocompletion request, satisfy it. This must be called
	// first before anything else since its possible to be autocompleting
	// -help or -version or other flags and we want to show completions
	// and not actually write the help or version.
	if c.autoComplete.Complete() {
		return EOK, nil
	}

	// Just show the version and exit if instructed.
	if c.isVersion && c.version != "" {
		return c.writeHelpString(c.version, 0)
	}

	// Just print the help when only '-h' or '--help' is passed
	if sc := c.subCommand; c.isHelp && sc == "" {
		return c.writeHelp(sc, helpErrCode{
			Success: 0,
			Failure: EPerm,
		})
	}

	// Autocomplete requires the "Name" to be set so that we know what command
	// to setup the autocomplete on.
	if c.name == "" {
		return EPerm, fmt.Errorf("name not set %q", c.name)
	}

	if c.isAutoCompleteInstall {
		if err := c.autoComplete.Install(c.name); err != nil {
			return EPerm, err
		}
	}
	if c.isAutoCompleteUninstall {
		if err := c.autoComplete.Uninstall(c.name); err != nil {
			return EPerm, err
		}
	}

	// Attempt to get the factory function for creating the command
	// implementation. If the command is invalid or blank, it is an error.
	command, ok := c.commands.Get(c.subCommand)
	if !ok {
		if c.subCommand == "" {
			c.ui.Output(c.header)
		}
		if hint, ok := c.commands.GetClosestName(c.subCommand); ok {
			c.ui.Output("Did you mean?\n")
			c.ui.Info(fmt.Sprintf("    %s\n", hint))
		}
		return c.writeHelp(c.subCommandParent(), helpErrCode{
			Success: EKeyExpired,
			Failure: EPerm,
		})
	}

	// If we've been instructed to just print the help, then print help
	if c.isHelp {
		return EOK, c.commandHelp(command)
	}

	// If there is an invalid flag, then error
	if len(c.commandFlags) > 0 {
		return EPerm, c.commandHelp(command)
	}

	// Run the command
	if err := command.FlagSet().Parse(c.subCommandArgs); err != nil {
		return EPerm, c.commandHelp(command)
	}

	code := command.Run()
	if code.ShowHelp {
		if len(c.subCommandArgs) == 1 {
			if hint, ok := c.commands.GetClosestName(fmt.Sprintf("%s %s", c.subCommand, c.subCommandArgs[0])); ok {
				c.ui.Output("Did you mean?\n")
				c.ui.Info(fmt.Sprintf("    %s\n", hint))
			}
		}
		return EPerm, c.commandHelp(command)
	}

	return code.Code, nil
}

// IsVersion returns whether or not the version flag is present within the
// arguments.
func (c *CLI) IsVersion() bool {
	return c.isVersion
}

// IsHelp returns whether or not the help flag is present within the arguments.
func (c *CLI) IsHelp() bool {
	return c.isHelp
}

// IsAutoCompleteInstall returns whether or not the auto complete install flag
// is present within the arguments.
func (c *CLI) IsAutoCompleteInstall() bool {
	return c.isAutoCompleteInstall
}

// IsAutoCompleteUninstall returns whether or not the auto complete install flag
// is present within the arguments.
func (c *CLI) IsAutoCompleteUninstall() bool {
	return c.isAutoCompleteUninstall
}

func (c *CLI) processArgs(args []string) error {
	for i, arg := range args {
		if arg == "--" {
			break
		}

		// Check for help flags
		if arg == "-h" || arg == "-help" || arg == "--help" {
			c.isHelp = true
			continue
		}

		if c.subCommand == "" {
			// Check for version flags if not in a subCommand.
			if arg == "-v" || arg == "-version" || arg == "--version" {
				c.isVersion = true
				continue
			}

			if arg == "-aci" || arg == "-autocomplete-install" || arg == "--autocomplete-install" {
				c.isAutoCompleteInstall = true
				continue
			}

			if arg == "-acu" || arg == "-autocomplete-uninstall" || arg == "--autocomplete-uninstall" {
				c.isAutoCompleteUninstall = true
				continue
			}

			if arg != "" && arg[0] == '-' {
				// Record the arg...
				c.commandFlags = append(c.commandFlags, arg)
			}
		}

		// If we didn't find a subCommand yet and this is the first non-flag
		// argument, then this is our subCommand.
		if c.subCommand == "" && arg != "" && arg[0] != '-' {
			c.subCommand = arg
			if c.commands.Nested() {
				// If the command has a space in it, then it is invalid.
				// Set a blank command so that it fails.
				if strings.ContainsRune(arg, ' ') {
					c.subCommand = ""
					return nil
				}

				// Determine the argument we look to end subCommands.
				// We look at all arguments until one has a space. This
				// disallows commands like: ./cli foo "bar baz".
				// An argument with a space is always an argument.
				var j int
				for k, v := range args[i:] {
					if strings.ContainsRune(v, ' ') {
						break
					}
					j = i + k + 1
				}

				// Nested CLI the subCommand is actually the entire arg list up
				// to a flag that is still a valid subCommand.
				searchKey := strings.Join(args[i:j], " ")
				k, ok := c.commands.LongestPrefix(searchKey)
				if ok {
					// k could be a prefix that doesn't contain the full command
					// such as "foo", instead of "foobar", so we need to verify
					// that we have an entire key. To do that, we look for an
					// ending in a space of end of a string.
					verify, err := regexp.Compile(regexp.QuoteMeta(k) + `( |$)`)
					if err != nil {
						return err
					}
					if verify.MatchString(searchKey) {
						c.subCommand = k
						i += strings.Count(k, " ")
					}
				}
			}

			// The remaining args the subCommand arguments
			c.subCommandArgs = args[i+1:]
		}
	}

	// If we never found a subCommand and support a default command, then
	// switch to using that
	if c.subCommand == "" {
		if _, ok := c.commands.Get(""); ok {
			x := c.commandFlags
			x = append(x, c.subCommandArgs...)
			c.commandFlags = nil
			c.subCommandArgs = x
		}
	}

	return nil
}

// helpCommands returns the subCommands for the HelpFunc argument.
// This will only contain immediate subCommands.
func (c *CLI) helpCommands(prefix string) (map[string]Command, error) {
	// if our prefix isn't empty, make sure it ends in ' '
	if prefix != "" && prefix[len(prefix)-1] != ' ' {
		prefix += " "
	}

	// Get all the subkeys of this command
	var keys []string
	c.commands.WalkPrefix(prefix, func(k string, v radix.Value) bool {
		// Ignore any sub-sub keys, i.e. "foo bar baz" when we want "foo bar"
		if !strings.Contains(k[len(prefix):], " ") {
			keys = append(keys, k)
		}

		return false
	})

	// For each of the keys return that in the map
	res := make(map[string]Command, len(keys))
	for _, k := range keys {
		cmd, ok := c.commands.Get(k)
		if !ok {
			return nil, fmt.Errorf("not found: %q", k)
		}

		// If this is a hidden command, don't show it
		if ok := c.commands.isHidden(k); ok {
			continue
		}

		res[k] = cmd
	}

	return res, nil
}

func (c *CLI) commandHelp(command Command) error {
	tmpl, err := newHelpTemplate(c.ui, command)
	if err != nil {
		return err
	}

	help := command.Help()
	if flagset := command.FlagSet(); flagset != nil {

		buf := new(bytes.Buffer)
		tab := tabwriter.NewWriter(buf, 2, 2, 4, ' ', 0)
		flagset.VisitAll(func(flag *flag.Flag) {
			tab.Write([]byte(fmt.Sprintf("  %s\t%s\t%s\t\n", flag.Name, flag.DefValue, flag.Usage)))
		})
		if err := tab.Flush(); err != nil {
			return errors.WithStack(err)
		}

		if b := buf.String(); b != "" {
			help = fmt.Sprintf("%s\nFlags:\n\n%s", help, b)
		}
	}

	// Template data
	data := helpTemplateData{
		Name: c.name,
		Help: help,
	}

	// Build the subCommand list if we have it
	if c.commands.Nested() {
		// Get the matching keys
		subCommands, err := c.helpCommands(c.subCommand)
		if err != nil {
			return err
		}

		keys := make([]string, 0, len(subCommands))
		for k := range subCommands {
			keys = append(keys, k)
		}

		// Sort they keys for deterministic output
		sort.Strings(keys)

		longest := findLongestString(keys)

		// Go through and create their structures
		for _, k := range keys {
			// Get the command
			sub, ok := subCommands[k]
			if !ok {
				return fmt.Errorf("error getting subcommand %q", k)
			}

			// Find the last space and make sure we only include that last part
			name := k
			if idx := strings.LastIndex(k, " "); idx >= 0 {
				name = name[idx+1:]
			}

			data.SubCommands = append(data.SubCommands, subHelpTemplateData{
				Name:        name,
				NameAligned: name + strings.Repeat("", longest-len(k)),
				Help:        sub.Help(),
				Synopsis:    sub.Synopsis(),
			})
		}
	}

	return tmpl.Render(data)
}

// subCommandParent returns the parent of this subCommand, if there is one.
// Returns empty string ("") if this isn't a parent.
func (c *CLI) subCommandParent() string {
	// get the subCommand, if it is "", just return
	sub := c.subCommand
	if sub == "" {
		return sub
	}

	// Clear any trailing spaces and find the last space
	sub = strings.TrimRight(sub, " ")
	idx := strings.LastIndex(sub, " ")
	if idx == -1 {
		// No space means our parent is the root
		return ""
	}
	return sub[:idx]
}

type helpErrCode struct {
	Success, Failure Errno
}

func (c *CLI) writeHelp(command string, errCode helpErrCode) (Errno, error) {
	cmd, err := c.helpCommands(command)
	if err != nil {
		return errCode.Failure, err
	}
	res, err := c.helpFunc(cmd)
	if err != nil {
		return errCode.Failure, err
	}
	return c.writeHelpString(res, errCode.Success)
}

func (c *CLI) writeHelpString(s string, code Errno) (Errno, error) {
	c.ui.Output(s)
	return code, nil
}

func findLongestString(s []string) (m int) {
	for _, k := range s {
		if v := len(k); v > m {
			m = v
		}
	}
	return
}

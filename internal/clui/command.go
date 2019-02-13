package clui

import (
	"flag"
	"fmt"
	"math"
	"strings"

	"github.com/spoke-d/thermionic/internal/clui/distance"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
	"github.com/spoke-d/thermionic/internal/clui/radix"
)

const defaultHelpText = "This command is accessed by using one of the subcommands below."

// ExitCode is a returnable code from the command, that can define documented
// behavioral changes.
type ExitCode struct {
	// Code represents exit codes that are returned.
	Code Errno

	// ShowHelp instructs the showing of the help after a command has been run.
	// It can be useful to give further information when needed.
	ShowHelp bool
}

// Command is a runnable sub-command of CLI.
type Command interface {

	// UI returns a UI for interaction.
	UI() UI

	// Flags returns the FlagSet associated with the command. All the flags are
	// parsed before running the command.
	FlagSet() *flagset.FlagSet

	// Help should return a long-form help text that includes the command-line
	// usage. A brief few sentences explaining the function of the command, and
	// the complete list of flags the command accepts.
	Help() string

	// Synopsis should return a one-line, short synopsis of the command.
	// This should be short (50 characters of less ideally).
	Synopsis() string

	// Run should run the actual command with the given CLI instance and
	// command-line arguments. It should return the exit status when it is
	// finished.
	//
	// There are a handful of special exit codes that can return documented
	// behavioral changes.
	Run() ExitCode
}

// TextCommand defines a simple text based command, that can be useful for
// creating commands that require more text based explanations.
type TextCommand struct {

	// HelpText defines the text for Help
	HelpText string

	// SynopsisText defines the text for Synopsis
	SynopsisText string

	ui      UI
	flagSet *flagset.FlagSet
}

// NewTextCommand creates a Command with sane defaults
func NewTextCommand(help, synopsis string) Command {
	return &TextCommand{
		HelpText:     help,
		SynopsisText: synopsis,
		ui:           NewNopUI(),
		flagSet:      flagset.NewFlagSet("text-command", flag.ExitOnError),
	}
}

// UI returns a UI for interaction.
func (c *TextCommand) UI() UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *TextCommand) FlagSet() *flagset.FlagSet {
	return c.flagSet
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *TextCommand) Help() string {
	return c.HelpText
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *TextCommand) Synopsis() string {
	return c.SynopsisText
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *TextCommand) Run() ExitCode {
	return ExitCode{
		Code:     0,
		ShowHelp: true,
	}
}

// Registry holds the commands in a central repository for easy access.
type Registry struct {
	commands       map[string]Command
	commandsHidden map[string]struct{}
	commandTree    *radix.Tree

	commandNested bool
}

// NewRegistry creates a Registry with sane defaults.
func NewRegistry() *Registry {
	return &Registry{
		commands:       make(map[string]Command),
		commandsHidden: make(map[string]struct{}),
		commandTree:    radix.New(),
	}
}

// Add a Command to the Registry for a given key. The key is normalized
// to remove trailing spaces for consistency.
// Returns an error when inserting into the Registry fails
func (r *Registry) Add(key string, cmd Command) error {
	k := normalizeKey(key)
	if _, _, err := r.commandTree.Insert(k, cmd); err != nil {
		return err
	}
	r.commands[k] = cmd
	if strings.ContainsRune(k, ' ') {
		r.commandNested = true
	}
	return nil
}

// Remove a Command from the Registry for a given key. The key is
// normalized to remove trailing spaces for consistency.
// Returns an error when deleting from the Registry upon failure
func (r *Registry) Remove(key string) (Command, error) {
	k := normalizeKey(key)

	cmd, ok := r.commands[k]
	if ok {
		delete(r.commands, k)
		// Make sure we put ourselves into a known state
		r.commandNested = r.hasNestedCommands()
	}

	if _, v := r.commandTree.Delete(k); ok && v {
		return cmd, nil
	}

	return nil, fmt.Errorf("no valid key found for %q", key)
}

// Get returns a Command for a given key. The key is normalized to remove
// trailing spaces for consistency.
// Returns true if it was found.
func (r *Registry) Get(key string) (Command, bool) {
	k := normalizeKey(key)
	if _, ok := r.commands[k]; !ok {
		return nil, false
	}
	raw, ok := r.commandTree.Get(k)
	if !ok {
		return nil, false
	}
	cmd, ok := raw.(Command)
	if !ok {
		return nil, false
	}
	return cmd, true
}

// GetClosestName returns the closest command to the given key
func (r *Registry) GetClosestName(key string) (string, bool) {
	if len(key) == 0 {
		return "", false
	}
	closest := struct {
		name     string
		distance int
	}{
		distance: math.MaxInt64,
	}
	r.commandTree.Walk(func(name string, value radix.Value) bool {
		d := distance.ComputeDistance(key, name)
		if strings.HasPrefix(name, key[:1]) && d < closest.distance {
			closest.name = name
			closest.distance = d
		}
		return false
	})
	if closest.name == "" {
		return "", false
	}
	k := normalizeKey(closest.name)
	if _, ok := r.commands[k]; !ok {
		return "", false
	}
	return k, true
}

// Walk is used to walk the command Registry
func (r *Registry) Walk(fn radix.WalkFn) {
	r.commandTree.Walk(fn)
}

// WalkPrefix is used to walk the tree under a prefix
func (r *Registry) WalkPrefix(prefix string, fn radix.WalkFn) {
	r.commandTree.WalkPrefix(prefix, fn)
}

// LongestPrefix is like Get, but instead of an exact match, it will return
// the longest prefix match.
func (r *Registry) LongestPrefix(key string) (string, bool) {
	k := normalizeKey(key)
	s, _, ok := r.commandTree.LongestPrefix(k)
	return s, ok
}

// Process runs through the registry and fills in any commands that are required
// for nesting (sub commands)
// Returns an error if there was an issue adding any commands to the underlying
// storage.
func (r *Registry) Process() error {
	if r.commandNested {
		var (
			walkFn radix.WalkFn
			insert = make(map[string]struct{})
		)

		walkFn = func(k string, v radix.Value) bool {
			idx := strings.LastIndex(k, " ")
			if idx == -1 {
				// If there is no space, just ignore top level commands
				return false
			}

			// Trim up to that space so we can get the expected parent
			k = k[:idx]
			if _, ok := r.Get(k); ok {
				return false
			}

			// We're missing the parent, so let's insert this
			insert[k] = struct{}{}

			// Call the walk function recursively so we check this one too
			return walkFn(k, nil)
		}
		r.Walk(walkFn)

		// Insert any that we're missing
		for k := range insert {
			if err := r.Add(k, NewTextCommand(fmt.Sprintf("%s\n", defaultHelpText), "")); err != nil {
				return err
			}
		}
	}
	return nil
}

// Nested returns a true if any commands with in the registry are nested.
func (r *Registry) Nested() bool {
	return r.commandNested
}

func (r *Registry) isHidden(key string) bool {
	k := normalizeKey(key)
	_, ok := r.commandsHidden[k]
	return ok
}

func (r *Registry) hasNestedCommands() bool {
	for k := range r.commands {
		if strings.ContainsRune(k, ' ') {
			return true
		}
	}
	return false
}

// normalizeKey attempts to normalize a command key before inserting it into a
// set of maps/trees. This should help with any possible inconsistencies when
// querying the structure.
func normalizeKey(k string) string {
	return strings.TrimSpace(k)
}

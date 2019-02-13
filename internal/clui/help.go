package clui

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// HelpFunc is the type of the function that is responsible for generating the
// help output when the CLI must show the general help text.
type HelpFunc func(map[string]Command) (string, error)

// BasicHelpFunc generates some bashic help output that is usually good enough
// for most CLI applications.
func BasicHelpFunc(name string) HelpFunc {
	return func(commands map[string]Command) (string, error) {
		var buf bytes.Buffer
		if _, err := buf.WriteString(fmt.Sprintf(
			"Usage: %s [--version] [--help] <command> [<args>]\n\n",
			name,
		)); err != nil {
			return "", err
		}
		if _, err := buf.WriteString("Available commands are:\n\n"); err != nil {
			return "", err
		}

		// Get the list of keys so we can sort the and also get the maximum key
		// length, so they can be aligned correctly.
		var (
			keys      = make([]string, 0, len(commands))
			maxKeyLen = 0
		)
		for key := range commands {
			if n := len(key); n > maxKeyLen {
				maxKeyLen = n
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			command, ok := commands[key]
			if !ok {
				return "", fmt.Errorf("command not found: %q", key)
			}

			key = fmt.Sprintf("%s%s", key, strings.Repeat(" ", maxKeyLen-len(key)))
			if _, err := buf.WriteString(fmt.Sprintf(
				"    %s    %s\n",
				key,
				command.Synopsis(),
			)); err != nil {
				return "", err
			}
		}

		return buf.String(), nil
	}
}

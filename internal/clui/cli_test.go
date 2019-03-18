package clui_test

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/quick"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
	"github.com/spoke-d/thermionic/internal/clui/install"
	"github.com/spoke-d/thermionic/internal/clui/mocks"
)

func TestVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		args    []string
		version bool
	}{
		{"skip short", []string{"--", "-v"}, false},
		{"skip long", []string{"--", "-version"}, false},
		{"skip long dash", []string{"--", "--version"}, false},
		{"version", []string{"-v"}, true},
		{"version long", []string{"-version"}, true},
		{"version long dash", []string{"--version"}, true},
		{"version with arg", []string{"-v", "foo"}, true},
		{"no version", []string{"foo", "bar"}, false},
		{"help", []string{"-h", "bar"}, false},
		{"arg then version", []string{"foo", "-v"}, false},
		{"arg then version long", []string{"foo", "-version"}, false},
		{"arg then version long dash", []string{"foo", "--version"}, false},
		{"sub version", []string{"foo", "--", "zip", "-v"}, false},
		{"sub version long", []string{"foo", "--", "zip", "-version"}, false},
		{"sub version long dash", []string{"foo", "--", "zip", "--version"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			version := "dev"
			buf := new(bytes.Buffer)
			cli := clui.NewCLI("cli", version, "", clui.CLIOptions{
				UI: clui.NewBasicUI(os.Stdin, buf),
			})
			code, err := cli.Run(tc.args)
			if err != nil {
				t.Fatal(err)
			}

			if expected, actual := tc.version, cli.IsVersion(); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if cli.IsVersion() {
				if expected, actual := clui.EOK, code; expected != actual {
					t.Errorf("expected: %d, actual: %d", expected, actual)
				}

				if expected, actual := version, strings.TrimRight(buf.String(), "\n"); expected != actual {
					t.Errorf("expected: %q, actual: %q", expected, actual)
				}
			}
		})
	}
}

func TestHelp(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		args []string
		help bool
	}{
		{"skip short", []string{"--", "-h"}, false},
		{"skip long", []string{"--", "-help"}, false},
		{"skip long dash", []string{"--", "--help"}, false},
		{"help", []string{"-h"}, true},
		{"help long", []string{"-help"}, true},
		{"help long dash", []string{"--help"}, true},
		{"help with arg", []string{"-h", "foo"}, true},
		{"no help", []string{"foo", "bar"}, false},
		{"help", []string{"-h", "bar"}, true},
		{"arg then help", []string{"foo", "-h"}, true},
		{"arg then help long", []string{"foo", "-help"}, true},
		{"arg then help long dash", []string{"foo", "--help"}, true},
		{"sub help", []string{"foo", "--", "zip", "-h"}, false},
		{"sub help long", []string{"foo", "--", "zip", "-help"}, false},
		{"sub help long dash", []string{"foo", "--", "zip", "--help"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
				UI: clui.NewBasicUI(os.Stdin, buf),
			})
			code, err := cli.Run(tc.args)
			if err != nil {
				t.Fatal(err)
			}

			if expected, actual := tc.help, cli.IsHelp(); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			want := clui.EPerm
			if cli.IsHelp() {
				want = clui.EOK
			}

			if expected, actual := want, code; actual < expected {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

		})
	}
}

func TestAutoCompleteInstall(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		args    []string
		install bool
	}{
		{"skip short", []string{"--", "-aci"}, false},
		{"skip long", []string{"--", "-autocomplete-install"}, false},
		{"skip long dash", []string{"--", "--autocomplete-install"}, false},
		{"install", []string{"-aci"}, true},
		{"install long", []string{"-autocomplete-install"}, true},
		{"install long dash", []string{"--autocomplete-install"}, true},
		{"install with arg", []string{"-autocomplete-install", "foo"}, true},
		{"no install", []string{"foo", "bar"}, false},
		{"install", []string{"-autocomplete-install", "bar"}, true},
		{"arg then install", []string{"foo", "-aci"}, false},
		{"arg then install long", []string{"foo", "-autocomplete-install"}, false},
		{"arg then install long dash", []string{"foo", "--autocomplete-install"}, false},
		{"sub install", []string{"foo", "--", "zip", "-aci"}, false},
		{"sub install long", []string{"foo", "--", "zip", "-autocomplete-install"}, false},
		{"sub install long dash", []string{"foo", "--", "zip", "--autocomplete-install"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
				UI:        clui.NewBasicUI(os.Stdin, buf),
				Installer: install.NewNop(),
			})
			code, err := cli.Run(tc.args)
			if err != nil {
				t.Fatal(err)
			}

			if expected, actual := tc.install, cli.IsAutoCompleteInstall(); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			want := clui.EPerm
			if cli.IsAutoCompleteInstall() {
				want = clui.EOK
			}

			if expected, actual := want, code; actual < expected {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

		})
	}
}

func TestAutoCompleteUninstall(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		args      []string
		uninstall bool
	}{
		{"skip short", []string{"--", "-acu"}, false},
		{"skip long", []string{"--", "-autocomplete-uninstall"}, false},
		{"skip long dash", []string{"--", "--autocomplete-uninstall"}, false},
		{"uninstall", []string{"-acu"}, true},
		{"uninstall long", []string{"-autocomplete-uninstall"}, true},
		{"uninstall long dash", []string{"--autocomplete-uninstall"}, true},
		{"uninstall with arg", []string{"-autocomplete-uninstall", "foo"}, true},
		{"no uninstall", []string{"foo", "bar"}, false},
		{"uninstall", []string{"-autocomplete-uninstall", "bar"}, true},
		{"arg then uninstall", []string{"foo", "-acu"}, false},
		{"arg then uninstall long", []string{"foo", "-autocomplete-uninstall"}, false},
		{"arg then uninstall long dash", []string{"foo", "--autocomplete-uninstall"}, false},
		{"sub uninstall", []string{"foo", "--", "zip", "-acu"}, false},
		{"sub uninstall long", []string{"foo", "--", "zip", "-autocomplete-uninstall"}, false},
		{"sub uninstall long dash", []string{"foo", "--", "zip", "--autocomplete-uninstall"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
				UI:        clui.NewBasicUI(os.Stdin, buf),
				Installer: install.NewNop(),
			})
			code, err := cli.Run(tc.args)
			if err != nil {
				t.Fatal(err)
			}

			if expected, actual := tc.uninstall, cli.IsAutoCompleteUninstall(); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			want := clui.EPerm
			if cli.IsAutoCompleteUninstall() {
				want = clui.EOK
			}

			if expected, actual := want, code; actual < expected {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

		})
	}
}

func TestCLI_Run(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := guard(func(name string) bool {
		flagSet := flagset.NewFlagSet("test", flag.ExitOnError)
		flagSet.String("bar", "", "bar flag")

		command := mocks.NewMockCommand(ctrl)
		command.EXPECT().FlagSet().Return(flagSet)
		command.EXPECT().Run().Return(clui.ExitCode{
			Code: 0,
		})

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name, command)

		_, err := cli.Run([]string{name, "-bar", "-baz"})
		if err != nil {
			t.Fatal(err)
		}
		return true
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestCLI_Run_Blank(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := guard2(func(name, val string) bool {
		flagSet := flagset.NewFlagSet("test", flag.ExitOnError)
		res := flagSet.String("bar", "", "bar flag")

		command := mocks.NewMockCommand(ctrl)
		command.EXPECT().FlagSet().Return(flagSet)
		command.EXPECT().Run().Return(clui.ExitCode{
			Code: 0,
		})

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name, command)

		_, err := cli.Run([]string{"", name, "-bar", val})
		if err != nil {
			t.Fatal(err)
		}
		return *res == val
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestCLI_Run_Prefix(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := guard(func(name string) bool {
		command := mocks.NewMockCommand(ctrl)
		command.EXPECT().Synopsis().Return("")

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name, command)
		cli.AddCommand(name+" bar", command)

		exitCode, err := cli.Run([]string{name + "bar"})
		if err != nil {
			t.Fatal(err)
		}
		return exitCode == clui.EKeyExpired
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestCLI_Run_Nested(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := guard(func(name string) bool {
		flagSet := flagset.NewFlagSet("test", flag.ExitOnError)
		flagSet.String("arg", "", "test arg")

		command := mocks.NewMockCommand(ctrl)
		command.EXPECT().FlagSet().Return(flagSet)
		command.EXPECT().Run().Return(clui.ExitCode{
			Code: 0,
		})

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name, mocks.NewMockCommand(ctrl))
		cli.AddCommand(name+" sub", command)

		exitCode, err := cli.Run([]string{name, "sub", "--arg", "--bar"})
		if err != nil {
			t.Fatal(err)
		}
		return exitCode == clui.EOK
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestCLI_Run_Top(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := guard(func(name string) bool {
		flagSet := flagset.NewFlagSet("test", flag.ExitOnError)

		command := mocks.NewMockCommand(ctrl)
		command.EXPECT().FlagSet().Return(flagSet)
		command.EXPECT().Run().Return(clui.ExitCode{
			Code: 0,
		})

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name, command)
		cli.AddCommand(name+" sub", mocks.NewMockCommand(ctrl))

		exitCode, err := cli.Run([]string{name})
		if err != nil {
			t.Fatal(err)
		}
		return exitCode == clui.EOK
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestCLI_Run_NestedNoArgs(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := guard(func(name string) bool {
		flagSet := flagset.NewFlagSet("test", flag.ExitOnError)

		command := mocks.NewMockCommand(ctrl)
		command.EXPECT().FlagSet().Return(flagSet)
		command.EXPECT().Run()

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name, mocks.NewMockCommand(ctrl))
		cli.AddCommand(name+" sub", command)

		exitCode, err := cli.Run([]string{name, "sub"})
		if err != nil {
			t.Fatal(err)
		}
		return exitCode == clui.EOK
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestCLI_Run_NestedQuotedCommand(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := guard(func(name string) bool {
		command := mocks.NewMockCommand(ctrl)
		command.EXPECT().Synopsis().Return(name)

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name, command)
		cli.AddCommand(name+" sub", mocks.NewMockCommand(ctrl))

		exitCode, err := cli.Run([]string{name + "sub"})
		if err != nil {
			t.Fatal(err)
		}
		return exitCode == clui.EKeyExpired
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestCLI_Run_NestedQuotedArg(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := guard2(func(name, sub string) bool {
		flagSet := flagset.NewFlagSet("test", flag.ExitOnError)

		command := mocks.NewMockCommand(ctrl)
		command.EXPECT().FlagSet().Return(flagSet)
		command.EXPECT().Run().Return(clui.ExitCode{
			Code: clui.EOK,
		})

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name, command)
		cli.AddCommand(fmt.Sprintf("%s %s", name, sub), mocks.NewMockCommand(ctrl))

		exitCode, err := cli.Run([]string{name, sub + " arg"})
		if err != nil {
			t.Fatal(err)
		}
		return exitCode == clui.EOK
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestCLI_Run_Help(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tc := range [][]string{
		{"-h"},
		{"--help"},
	} {
		var (
			buf      = new(bytes.Buffer)
			helpText = "foo"

			cli = clui.NewCLI("test", "dev", "", clui.CLIOptions{
				HelpFunc: func(map[string]clui.Command) (string, error) {
					return helpText, nil
				},
				UI: clui.NewBasicUI(os.Stdin, buf),
			})
		)

		code, err := cli.Run(tc)
		if err != nil {
			t.Errorf("expected: %#v, actual: %s", tc, err)
			continue
		}

		if expected, actual := clui.EOK, code; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if expected, actual := helpText, buf.String(); !strings.Contains(actual, expected) {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}
}

func TestCLI_Run_IllegalHelp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helpText := "foo"

	command := mocks.NewMockCommand(ctrl)
	command.EXPECT().Help().Return(helpText)
	command.EXPECT().FlagSet().Return(nil)

	for _, tc := range []struct {
		args []string
		exit clui.Errno
		cmd  clui.Command
	}{
		{nil, clui.EKeyExpired, command},
		{[]string{"i-do-not-exist"}, clui.EKeyExpired, command},
		{[]string{"-bad-flag", "foo"}, clui.EPerm, command},
	} {
		var (
			buf = new(bytes.Buffer)

			cli = clui.NewCLI("test", "dev", "", clui.CLIOptions{
				HelpFunc: func(map[string]clui.Command) (string, error) {
					return helpText, nil
				},
				UI: clui.NewBasicUI(os.Stdin, buf),
			})
		)

		cli.AddCommand("foo", tc.cmd)

		code, err := cli.Run(tc.args)
		if err != nil {
			t.Errorf("expected: %#v, actual: %s", tc, err)
			continue
		}

		if expected, actual := tc.exit, code; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if expected, actual := helpText, buf.String(); !strings.Contains(actual, expected) {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}
}

func TestCLI_Run_CommandHelp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helpText := "foo"

	command := mocks.NewMockCommand(ctrl)
	command.EXPECT().Help().Return(helpText).Times(2)
	command.EXPECT().FlagSet().Return(nil).Times(2)

	for _, tc := range []struct {
		args []string
		exit clui.Errno
		cmd  clui.Command
	}{
		{nil, clui.EKeyExpired, command},
		{[]string{"--help", "foo"}, clui.EOK, command},
		{[]string{"-h", "foo"}, clui.EOK, command},
	} {
		var (
			buf = new(bytes.Buffer)

			cli = clui.NewCLI("test", "dev", "", clui.CLIOptions{
				HelpFunc: func(map[string]clui.Command) (string, error) {
					return helpText, nil
				},
				UI: clui.NewBasicUI(os.Stdin, buf),
			})
		)

		cli.AddCommand("foo", tc.cmd)

		code, err := cli.Run(tc.args)
		if err != nil {
			t.Errorf("expected: %#v, actual: %s", tc, err)
			continue
		}

		if expected, actual := tc.exit, code; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if expected, actual := helpText, buf.String(); !strings.Contains(actual, expected) {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}
}

func TestCLI_Run_CommandNestedHelp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helpText := "foo"

	command := mocks.NewMockCommand(ctrl)
	command.EXPECT().Help().Return(helpText).Times(2)
	command.EXPECT().FlagSet().Return(nil).Times(2)

	for _, tc := range []struct {
		args []string
		exit clui.Errno
		cmd  clui.Command
	}{
		{nil, clui.EKeyExpired, command},
		{[]string{"--help", "foo", "bar"}, clui.EOK, command},
		{[]string{"-h", "foo", "bar"}, clui.EOK, command},
	} {
		var (
			buf = new(bytes.Buffer)

			cli = clui.NewCLI("test", "dev", "", clui.CLIOptions{
				HelpFunc: func(map[string]clui.Command) (string, error) {
					return helpText, nil
				},
				UI: clui.NewBasicUI(os.Stdin, buf),
			})
		)

		cli.AddCommand("foo bar", tc.cmd)

		code, err := cli.Run(tc.args)
		if err != nil {
			t.Errorf("expected: %#v, actual: %s", tc, err)
			continue
		}

		if expected, actual := tc.exit, code; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if expected, actual := helpText, buf.String(); !strings.Contains(actual, expected) {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}
}

func TestCLI_Run_SubCommandHelp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helpText := "foo"

	command := mocks.NewMockCommand(ctrl)
	command.EXPECT().Help().Return(helpText).Times(2)
	command.EXPECT().FlagSet().Return(nil).Times(2)

	for _, tc := range []struct {
		args []string
		exit clui.Errno
		cmd  clui.Command
	}{
		{[]string{"--help", "foo"}, clui.EOK, command},
		{[]string{"-h", "foo"}, clui.EOK, command},
	} {
		var (
			buf = new(bytes.Buffer)

			cli = clui.NewCLI("test", "dev", "", clui.CLIOptions{
				HelpFunc: func(map[string]clui.Command) (string, error) {
					return helpText, nil
				},
				UI: clui.NewBasicUI(os.Stdin, buf),
			})
		)

		c := mocks.NewMockCommand(ctrl)
		c.EXPECT().Help().Return("hi!").Times(4)
		c.EXPECT().Synopsis().Return("hi!").Times(4)

		cli.AddCommand("foo", tc.cmd)
		cli.AddCommand("foo bar", c)
		cli.AddCommand("foo zip", c)
		cli.AddCommand("foo zap", c)
		cli.AddCommand("foo longer", c)
		cli.AddCommand("foo longer longest", c)

		code, err := cli.Run(tc.args)
		if err != nil {
			t.Errorf("expected: %#v, actual: %s", tc, err)
			continue
		}

		if expected, actual := tc.exit, code; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if expected, actual := helpText, buf.String(); !strings.Contains(actual, expected) {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}
}

func TestCLI_Run_SubCommandHelpNested(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helpText := "foo"

	command := mocks.NewMockCommand(ctrl)
	command.EXPECT().Help().Return(helpText).Times(2)
	command.EXPECT().FlagSet().Return(nil).Times(2)

	for _, tc := range []struct {
		args []string
		exit clui.Errno
		cmd  clui.Command
	}{
		{[]string{"--help", "l0"}, clui.EOK, command},
		{[]string{"-h", "l0"}, clui.EOK, command},
	} {
		var (
			buf = new(bytes.Buffer)

			cli = clui.NewCLI("test", "dev", "", clui.CLIOptions{
				HelpFunc: func(map[string]clui.Command) (string, error) {
					return helpText, nil
				},
				UI: clui.NewBasicUI(os.Stdin, buf),
			})
		)

		c := mocks.NewMockCommand(ctrl)
		c.EXPECT().Help().Return("hi!").Times(2)
		c.EXPECT().Synopsis().Return("hi!").Times(2)

		cli.AddCommand("l0", tc.cmd)
		cli.AddCommand("l0 l1a", c)
		cli.AddCommand("l0 l1b", c)
		cli.AddCommand("l0 l1a l3a", c)
		cli.AddCommand("l0 l1a l3b", c)

		code, err := cli.Run(tc.args)
		if err != nil {
			t.Errorf("expected: %#v, actual: %s", tc, err)
			continue
		}

		if expected, actual := tc.exit, code; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if expected, actual := helpText, buf.String(); !strings.Contains(actual, expected) {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}
}

func TestCLI_Help_Nested(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := guard(func(name string) bool {
		var called bool

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			HelpFunc: func(m map[string]clui.Command) (string, error) {
				called = true

				var keys []string
				for k := range m {
					keys = append(keys, k)
				}
				sort.Strings(keys)

				expected := []string{name}
				if !reflect.DeepEqual(keys, expected) {
					return "", fmt.Errorf("error: contained sub: %#v", keys)
				}

				return "", nil
			},
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name+" sub", mocks.NewMockCommand(ctrl))

		exitCode, err := cli.Run([]string{"--help"})
		if err != nil {
			t.Fatal(err)
		}
		return called && exitCode == clui.EOK
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestCLI_Help_MissingParent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helpText := `This command is accessed by using one of the subcommands below.

Subcommands:

  sub  synopsis for sub  

`

	fn := guard(func(name string) bool {
		command := mocks.NewMockCommand(ctrl)
		command.EXPECT().Help().Return("")
		command.EXPECT().Synopsis().Return("synopsis for sub")

		buf := new(bytes.Buffer)
		cli := clui.NewCLI("cli", "dev", "", clui.CLIOptions{
			UI: clui.NewBasicUI(os.Stdin, buf),
		})
		cli.AddCommand(name+" sub", command)

		exitCode, err := cli.Run([]string{name})
		if err != nil {
			t.Fatal(err)
		}
		if expected, actual := helpText, buf.String(); expected != actual {
			t.Errorf("expected: %q, actual: %q", expected, actual)
		}
		return exitCode == clui.EPerm
	})
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

// guard against empty names or names with spaces in them
func guard(fn func(string) bool) func(string) bool {
	return func(name string) bool {
		if name == "" {
			return true
		}
		return fn(strings.Replace(name, " ", "", -1))
	}
}

func guard2(fn func(string, string) bool) func(string, string) bool {
	return func(name, val string) bool {
		if name == "" || val == "" {
			return true
		}
		return fn(strings.Replace(name, " ", "", -1), strings.Replace(val, " ", "", -1))
	}
}

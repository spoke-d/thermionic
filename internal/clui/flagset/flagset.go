package flagset

import (
	"bufio"
	"bytes"
	"flag"
	"io"
	"io/ioutil"
	"strings"
	"syscall"
	"time"
)

// A FlagSet represents a set of defined flags. The zero value of a FlagSet
// has no name and has ContinueOnError error handling.
type FlagSet struct {
	flag             *flag.FlagSet
	Usage            func()
	src, args, flags []string
}

// NewFlagSet returns a new, empty flag set with the specified name and error
// handling property.
func NewFlagSet(name string, errorHandling flag.ErrorHandling) *FlagSet {
	return &FlagSet{
		flag: flag.NewFlagSet(name, errorHandling),
	}
}

// SetOutput sets the destination for usage and error messages.
// If output is nil, os.Stderr is used.
func (f *FlagSet) SetOutput(output io.Writer) {
	f.flag.SetOutput(output)
}

// VisitAll visits the flags in lexicographical order, calling fn for each.
// It visits all flags, even those not set.
func (f *FlagSet) VisitAll(fn func(*flag.Flag)) {
	f.flag.VisitAll(fn)
}

// Visit visits the flags in lexicographical order, calling fn for each.
// It visits only those flags that have been set.
func (f *FlagSet) Visit(fn func(*flag.Flag)) {
	f.flag.Visit(fn)
}

// Lookup returns the Flag structure of the named flag, returning nil if none exists.
func (f *FlagSet) Lookup(name string) *flag.Flag {
	return f.flag.Lookup(name)
}

// Set sets the value of the named flag.
func (f *FlagSet) Set(name, value string) error {
	return f.flag.Set(name, value)
}

// PrintDefaults prints to standard error the default values of all
// defined command-line flags in the set. See the documentation for
// the global function PrintDefaults for more information.
func (f *FlagSet) PrintDefaults() {
	f.flag.PrintDefaults()
}

// NFlag returns the number of flags that have been set.
func (f *FlagSet) NFlag() int {
	return len(f.flags)
}

// Flags returns the flag arguments.
func (f *FlagSet) Flags() []string {
	return f.flags
}

// Arg returns the i'th argument. Arg(0) is the first remaining argument
// after flags have been processed. Arg returns an empty string if the
// requested element does not exist.
func (f *FlagSet) Arg(i int) string {
	if i < 0 || i >= len(f.args) {
		return ""
	}
	return f.args[i]
}

// NArg is the number of arguments remaining after flags have been processed.
func (f *FlagSet) NArg() int {
	return len(f.args)
}

// Args returns the non-flag arguments.
func (f *FlagSet) Args() []string {
	return f.args
}

// Source returns the source arguments.
func (f *FlagSet) Source() []string {
	return f.src
}

// BoolVar defines a bool flag with specified name, default value, and usage string.
// The argument p points to a bool variable in which to store the value of the flag.
func (f *FlagSet) BoolVar(p *bool, name string, value bool, usage string) {
	f.flag.BoolVar(p, name, value, usage)
}

// Bool defines a bool flag with specified name, default value, and usage string.
// The return value is the address of a bool variable that stores the value of the flag.
func (f *FlagSet) Bool(name string, value bool, usage string) *bool {
	return f.flag.Bool(name, value, usage)
}

// IntVar defines an int flag with specified name, default value, and usage string.
// The argument p points to an int variable in which to store the value of the flag.
func (f *FlagSet) IntVar(p *int, name string, value int, usage string) {
	f.flag.IntVar(p, name, value, usage)
}

// Int defines an int flag with specified name, default value, and usage string.
// The return value is the address of an int variable that stores the value of the flag.
func (f *FlagSet) Int(name string, value int, usage string) *int {
	return f.flag.Int(name, value, usage)
}

// Int64Var defines an int64 flag with specified name, default value, and usage string.
// The argument p points to an int64 variable in which to store the value of the flag.
func (f *FlagSet) Int64Var(p *int64, name string, value int64, usage string) {
	f.flag.Int64Var(p, name, value, usage)
}

// Int64 defines an int64 flag with specified name, default value, and usage string.
// The return value is the address of an int64 variable that stores the value of the flag.
func (f *FlagSet) Int64(name string, value int64, usage string) *int64 {
	return f.flag.Int64(name, value, usage)
}

// UintVar defines a uint flag with specified name, default value, and usage string.
// The argument p points to a uint variable in which to store the value of the flag.
func (f *FlagSet) UintVar(p *uint, name string, value uint, usage string) {
	f.flag.UintVar(p, name, value, usage)
}

// Uint defines a uint flag with specified name, default value, and usage string.
// The return value is the address of a uint  variable that stores the value of the flag.
func (f *FlagSet) Uint(name string, value uint, usage string) *uint {
	return f.flag.Uint(name, value, usage)
}

// Uint64Var defines a uint64 flag with specified name, default value, and usage string.
// The argument p points to a uint64 variable in which to store the value of the flag.
func (f *FlagSet) Uint64Var(p *uint64, name string, value uint64, usage string) {
	f.flag.Uint64Var(p, name, value, usage)
}

// Uint64 defines a uint64 flag with specified name, default value, and usage string.
// The return value is the address of a uint64 variable that stores the value of the flag.
func (f *FlagSet) Uint64(name string, value uint64, usage string) *uint64 {
	return f.flag.Uint64(name, value, usage)
}

// StringVar defines a string flag with specified name, default value, and usage string.
// The argument p points to a string variable in which to store the value of the flag.
func (f *FlagSet) StringVar(p *string, name string, value string, usage string) {
	f.flag.StringVar(p, name, value, usage)
}

// String defines a string flag with specified name, default value, and usage string.
// The return value is the address of a string variable that stores the value of the flag.
func (f *FlagSet) String(name string, value string, usage string) *string {
	return f.flag.String(name, value, usage)
}

// Float64Var defines a float64 flag with specified name, default value, and usage string.
// The argument p points to a float64 variable in which to store the value of the flag.
func (f *FlagSet) Float64Var(p *float64, name string, value float64, usage string) {
	f.flag.Float64Var(p, name, value, usage)
}

// Float64 defines a float64 flag with specified name, default value, and usage string.
// The return value is the address of a float64 variable that stores the value of the flag.
func (f *FlagSet) Float64(name string, value float64, usage string) *float64 {
	return f.flag.Float64(name, value, usage)
}

// DurationVar defines a time.Duration flag with specified name, default value, and usage string.
// The argument p points to a time.Duration variable in which to store the value of the flag.
// The flag accepts a value acceptable to time.ParseDuration.
func (f *FlagSet) DurationVar(p *time.Duration, name string, value time.Duration, usage string) {
	f.flag.DurationVar(p, name, value, usage)
}

// Duration defines a time.Duration flag with specified name, default value, and usage string.
// The return value is the address of a time.Duration variable that stores the value of the flag.
// The flag accepts a value acceptable to time.ParseDuration.
func (f *FlagSet) Duration(name string, value time.Duration, usage string) *time.Duration {
	return f.flag.Duration(name, value, usage)
}

// Var defines a flag with the specified name and usage string. The type and
// value of the flag are represented by the first argument, of type Value, which
// typically holds a user-defined implementation of Value. For instance, the
// caller could create a flag that turns a comma-separated string into a slice
// of strings by giving the slice the methods of Value; in particular, Set would
// decompose the comma-separated string into the slice.
func (f *FlagSet) Var(value flag.Value, name string, usage string) {
	f.flag.Var(value, name, usage)
}

// Parse parses flag definitions from the argument list, which should not
// include the command name. Must be called after all flags in the FlagSet
// are defined and before flags are accessed by the program.
// The return value will be ErrHelp if -help or -h were set but not defined.
func (f *FlagSet) Parse(arguments []string) error {
	if f.Usage != nil {
		f.flag.Usage = f.Usage
	}

	if err := f.flag.Parse(arguments); err != nil {
		return err
	}

	f.src = arguments[:]

	flags := make(map[string]struct{})
	for _, v := range f.flag.Args() {
		if v == "-" || !strings.HasPrefix(v, "-") {
			f.args = append(f.args, v)
		} else {
			var i int
			name := strings.TrimLeftFunc(v, func(r rune) bool {
				i++
				if i <= 2 {
					return r == '-'
				}
				return false
			})
			flags[name] = struct{}{}
		}
	}

	fileArgs := make(map[string]string)
	if fileName, ok := syscall.Getenv("ENV_FILE"); ok && fileName != "" {
		if file, err := ioutil.ReadFile(fileName); err == nil {
			scanner := bufio.NewScanner(bytes.NewReader(file))
			for scanner.Scan() {
				parts := strings.Split(scanner.Text(), "=")
				if len(parts) > 1 && parts[0] != "" {
					fileArgs[parts[0]] = strings.Join(parts[1:], "=")
				}
			}
		}
	}

	f.Visit(func(flag *flag.Flag) {
		name := envName(flag.Name)
		if value, ok := fileArgs[name]; ok {
			f.flag.Set(flag.Name, value)
		}
		if value, ok := syscall.Getenv(name); ok {
			f.flag.Set(flag.Name, value)
		}
		flags[flag.Name] = struct{}{}
	})

	for k := range flags {
		f.flags = append(f.flags, k)
	}

	return nil
}

func envName(name string) string {
	return strings.Replace(strings.ToUpper(name), ".", "_", -1)
}

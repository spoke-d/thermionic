package style

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spoke-d/thermionic/internal/clui/isatty"
)

var (
	// NoStyle defines if the output is styleized or not. It's dynamically set to
	// false or true based on the stdout's file descriptor referring to a terminal
	// or not. This is a global option and affects all styles. For more control
	// over each style block use the methods DisableStyle() individually.
	NoStyle = os.Getenv("TERM") == "dumb" ||
		(!isatty.IsTerminal(os.Stdout.Fd()) && !isatty.IsCygwinTerminal(os.Stdout.Fd()))
)

// Attribute defines a single SGR Code
type Attribute int

const defaultEscapeChars = "\x1b"

// Base attributes
const (
	Reset Attribute = iota
	Bold
	Faint
	Italic
	Underline
	BlinkSlow
	BlinkRapid
	ReverseVideo
	Concealed
	CrossedOut
)

// Foreground text styles
const (
	FgBlack Attribute = iota + 30
	FgRed
	FgGreen
	FgYellow
	FgBlue
	FgMagenta
	FgCyan
	FgWhite
)

// Foreground Hi-Intensity text styles
const (
	FgHiBlack Attribute = iota + 90
	FgHiRed
	FgHiGreen
	FgHiYellow
	FgHiBlue
	FgHiMagenta
	FgHiCyan
	FgHiWhite
)

// Background text styles
const (
	BgBlack Attribute = iota + 40
	BgRed
	BgGreen
	BgYellow
	BgBlue
	BgMagenta
	BgCyan
	BgWhite
)

// Background Hi-Intensity text styles
const (
	BgHiBlack Attribute = iota + 100
	BgHiRed
	BgHiGreen
	BgHiYellow
	BgHiBlue
	BgHiMagenta
	BgHiCyan
	BgHiWhite
)

// Style defines a custom style object which is defined by SGR parameters.
type Style struct {
	params  []Attribute
	noStyle *bool
}

// New returns a newly created style object.
func New(value ...Attribute) *Style {
	return &Style{params: value}
}

// Format encodes a message by wrapping the value with a style.
func (c *Style) Format(a string) string {
	return c.wrap(a)
}

// wrap wraps the s string with the styles attributes. The string is ready to
// be printed.
func (c *Style) wrap(s string) string {
	if c.isNoStyleSet() {
		return s
	}

	return fmt.Sprintf("%s%s%s", c.format(), s, c.unformat())
}

func (c *Style) format() string {
	return fmt.Sprintf("%s[%sm", defaultEscapeChars, c.sequence())
}

func (c *Style) unformat() string {
	return fmt.Sprintf("%s[0m", defaultEscapeChars)
}

func (c *Style) isNoStyleSet() bool {
	// check first if we have user setted action
	if c.noStyle != nil {
		return *c.noStyle
	}

	// if not return the global option, which is disabled by default
	return NoStyle
}

// sequence returns a formatted SGR sequence to be plugged into a "\x1b[...m"
// an example output might be: "1;36" -> bold cyan
func (c *Style) sequence() string {
	format := make([]string, len(c.params))
	for i, v := range c.params {
		format[i] = strconv.Itoa(int(v))
	}

	return strings.Join(format, ";")
}

package clui

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"

	"github.com/spoke-d/thermionic/internal/ask"
	"github.com/spoke-d/thermionic/internal/clui/isatty"
	"github.com/spoke-d/thermionic/internal/clui/style"
)

// UI is an interface for interacting with the terminal, or "interface"
// of a CLI.
type UI interface {
	// Ask asks the user for input using the given query. The response is
	// returned as the given string, or an error.
	Ask(string) (string, error)

	// AskSecret asks the user for input using the given query, but does not echo
	// the keystrokes to the terminal.
	AskSecret(string) (string, error)

	// Output is called for normal standard output.
	Output(string)

	// Info is called for information related to the previous output.
	// In general this may be the exact same as Output, but this gives
	// UI implementors some flexibility with output formats.
	Info(string)

	// Error is used for any error messages that might appear on standard
	// error.
	Error(string)

	// Warn is used for any warning messages that might appear on standard
	// error.
	Warn(string)
}

// BasicUI is an implementation of UI that just outputs to the given writer.
type BasicUI struct {
	Reader      io.Reader
	Writer      io.Writer
	ErrorWriter io.Writer
}

// NewBasicUI creates a new BasicUI with dependencies.
func NewBasicUI(reader io.Reader, writer io.Writer) *BasicUI {
	return &BasicUI{
		Reader:      reader,
		Writer:      writer,
		ErrorWriter: nil,
	}
}

// Ask asks the user for input using the given query. The response is
// returned as the given string, or an error.
func (u *BasicUI) Ask(query string) (string, error) {
	return u.ask(query, false)
}

// AskSecret asks the user for input using the given query, but does not echo
// the keystrokes to the terminal.
func (u *BasicUI) AskSecret(query string) (string, error) {
	return u.ask(query, true)
}

// Error is used for any error messages that might appear on standard
// error.
func (u *BasicUI) Error(message string) {
	w := u.Writer
	if u.ErrorWriter != nil {
		w = u.ErrorWriter
	}

	fmt.Fprintln(w, message)
}

// Info is called for information related to the previous output.
// In general this may be the exact same as Output, but this gives
// UI implementors some flexibility with output formats.
func (u *BasicUI) Info(message string) {
	u.Output(message)
}

// Output is called for normal standard output.
func (u *BasicUI) Output(message string) {
	fmt.Fprintln(u.Writer, message)
}

// Warn is used for any warning messages that might appear on standard
// error.
func (u *BasicUI) Warn(message string) {
	u.Error(message)
}

func (u *BasicUI) ask(query string, secret bool) (string, error) {
	if _, err := fmt.Fprint(u.Writer, query+" "); err != nil {
		return "", err
	}

	// Register for interrupts so that we can catch it and immediately
	// return...
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	// Ask for input in a go-routine so that we can ignore it.
	var (
		errCh  = make(chan error, 1)
		lineCh = make(chan string, 1)
	)
	go func() {
		var (
			line string
			err  error
		)
		if secret && isatty.IsTerminal(os.Stdin.Fd()) {
			asker := ask.New(u.Reader, u.Writer)
			line, err = asker.Password("")
		} else {
			r := bufio.NewReader(u.Reader)
			line, err = r.ReadString('\n')
		}
		if err != nil {
			errCh <- err
			return
		}

		lineCh <- strings.TrimRight(line, "\r\n")
	}()

	select {
	case err := <-errCh:
		return "", err
	case line := <-lineCh:
		return line, nil
	case <-sigCh:
		// Print a newline so that any further output starts properly
		// on a new line.
		fmt.Fprintln(u.Writer)

		return "", errors.New("interrupted")
	}
}

// ColorUI is a UI implementation that colors its output according
// to the given color schemes for the given type of output.
type ColorUI struct {
	OutputColor                      *style.Style
	InfoColor, ErrorColor, WarnColor *style.Style
	UI                               UI
}

// NewColorUI creates a new ColorUI with dependencies.
func NewColorUI(ui UI) *ColorUI {
	return &ColorUI{
		UI: ui,
	}
}

// Ask asks the user for input using the given query. The response is
// returned as the given string, or an error.
func (u *ColorUI) Ask(query string) (string, error) {
	return u.UI.Ask(u.format(query, u.OutputColor))
}

// AskSecret asks the user for input using the given query, but does not echo
// the keystrokes to the terminal.
func (u *ColorUI) AskSecret(query string) (string, error) {
	return u.UI.AskSecret(u.format(query, u.OutputColor))
}

// Output is called for normal standard output.
func (u *ColorUI) Output(message string) {
	u.UI.Output(u.format(message, u.OutputColor))
}

// Info is called for information related to the previous output.
// In general this may be the exact same as Output, but this gives
// UI implementors some flexibility with output formats.
func (u *ColorUI) Info(message string) {
	u.UI.Info(u.format(message, u.InfoColor))
}

// Error is used for any error messages that might appear on standard
// error.
func (u *ColorUI) Error(message string) {
	u.UI.Error(u.format(message, u.ErrorColor))
}

// Warn is used for any warning messages that might appear on standard
// error.
func (u *ColorUI) Warn(message string) {
	u.UI.Warn(u.format(message, u.WarnColor))
}

func (u *ColorUI) format(message string, sty *style.Style) string {
	if sty == nil {
		return message
	}
	return sty.Format(message)
}

// NewNopUI creates a new UI that doesn't read or write anywhere. This is useful
// for testing or for text commands that should prevent any output.
func NewNopUI() UI {
	return NewBasicUI(nopReader{}, ioutil.Discard)
}

// UIWriter is an io.Writer implementation that can be used with
// loggers that writes every line of log output data to a UI at the
// Info level.
type UIWriter struct {
	UI UI
}

func (w *UIWriter) Write(b []byte) (n int, err error) {
	n = len(b)

	// remove any of the trailing new lines
	w.UI.Info(strings.TrimSuffix(string(b), "\n"))
	return n, nil
}

// nopReader creates a io.Reader that doesn't read anything
type nopReader struct{}

func (n nopReader) Read([]byte) (int, error) {
	return 0, nil
}

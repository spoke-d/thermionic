package ask

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	yes = []string{"yes", "y"}
	no  = []string{"no", "n"}
)

// ErrMaxRetriesInput represents when an input is invalid after numerous
// attempts
var ErrMaxRetriesInput = errors.New("invalid number input tried")

// Ask represents a way of asking questions and getting answers from the
// output and input terminal retrospectively.
type Ask struct {
	input        *bufio.Scanner
	output       io.Writer
	invalidInput func(string)
	retryCount   int
}

// New creates a new Ask with sane defaults.
func New(input io.Reader, output io.Writer) *Ask {
	return &Ask{
		input:  bufio.NewScanner(input),
		output: output,
		invalidInput: func(answer string) {
			fmt.Fprintf(output, "Invalid input, try again.\n\n")
		},
		retryCount: 10,
	}
}

// Bool asks a question and expect a yes/no answer.
func (a *Ask) Bool(question, defaultAnswer string) (bool, error) {
	for i := 0; i < a.retryCount; i++ {
		answer := a.askQuestion(question, defaultAnswer, a.readAnswer)
		if contains(strings.ToLower(answer), yes) {
			return true, nil
		} else if contains(strings.ToLower(answer), no) {
			return false, nil
		}
		a.invalidInput(answer)
	}
	return false, ErrMaxRetriesInput
}

// Choice asks the user to select one of multiple options
func (a *Ask) Choice(question string, choices []string, defaultAnswer string) (string, error) {
	for i := 0; i < a.retryCount; i++ {
		answer := a.askQuestion(question, defaultAnswer, a.readAnswer)
		if contains(answer, choices) {
			return answer, nil
		}
		a.invalidInput(answer)
	}
	return "", ErrMaxRetriesInput
}

// Int asks the user to enter an integer between a min and max value
func (a *Ask) Int(question string, min, max int64, defaultAnswer string) (int64, error) {
	for i := 0; i < a.retryCount; i++ {
		answer := a.askQuestion(question, defaultAnswer, a.readAnswer)
		result, err := strconv.ParseInt(answer, 10, 64)
		if err == nil && (min == -1 || result >= min) && (max == -1 || result <= max) {
			return result, nil
		}
		a.invalidInput(answer)
	}
	return -1, ErrMaxRetriesInput
}

// String asks the user to enter a string, which optionally
// conforms to a validation function.
func (a *Ask) String(question, defaultAnswer string, validate func(string) error) (string, error) {
	for i := 0; i < a.retryCount; i++ {
		answer := a.askQuestion(question, defaultAnswer, a.readAnswer)
		if validate != nil {
			if err := validate(answer); err != nil {
				fmt.Fprintf(a.output, "Invalid input: %s\n\n", err)
				continue
			}
			return answer, nil
		}
		if len(answer) != 0 {
			return answer, nil
		}
		a.invalidInput(answer)
	}
	return "", ErrMaxRetriesInput
}

// Text asks the user to enter a string, which optionally
// conforms to a validation function.
func (a *Ask) Text(question string) (string, error) {
	for i := 0; i < a.retryCount; i++ {
		answer := a.askQuestion(question, "", a.readTextAnswer)
		if len(answer) != 0 {
			return answer, nil
		}
		a.invalidInput(answer)
	}
	return "", ErrMaxRetriesInput
}

// Password asks the user to enter a password.
func (a *Ask) Password(question string) (string, error) {
	for i := 0; i < a.retryCount; i++ {
		fmt.Fprintf(a.output, question)

		pwd, _ := terminal.ReadPassword(0)
		fmt.Fprintf(a.output, "")
		inFirst := string(pwd)
		inFirst = strings.TrimSuffix(inFirst, "\n")

		fmt.Fprintf(a.output, "Again: ")
		pwd, _ = terminal.ReadPassword(0)

		fmt.Fprintf(a.output, "")
		inSecond := string(pwd)
		inSecond = strings.TrimSuffix(inSecond, "\n")

		if inFirst == inSecond {
			return inFirst, nil
		}
		a.invalidInput("")
	}
	return "", ErrMaxRetriesInput
}

// PasswordOnce asks the user to enter a password.
func (a *Ask) PasswordOnce(question string) string {
	fmt.Fprintf(a.output, question)

	pwd, _ := terminal.ReadPassword(0)
	fmt.Fprintf(a.output, "")

	return strings.TrimSuffix(string(pwd), "\n")
}

// Ask a question on the output stream and read the answer from the input stream
func (a *Ask) askQuestion(question, defaultAnswer string, read func(string) string) string {
	fmt.Fprintf(a.output, question)
	return read(defaultAnswer)
}

// Read the user's answer from the input stream, trimming newline and providing a default.
func (a *Ask) readAnswer(defaultAnswer string) string {
	// we don't care about the error here, as we get the answer up to the
	// error occurred
	var answer string
	for a.input.Scan() {
		answer = a.input.Text()
		break
	}
	answer = strings.TrimSuffix(answer, "\n")
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return defaultAnswer
	}
	return answer
}

// Read the user's text answer from the input stream, this doesn't trim new lines
// or provide a default text
func (a *Ask) readTextAnswer(defaultAnswer string) string {
	var answer string
	for a.input.Scan() {
		answer += fmt.Sprintf("%s\n", a.input.Text())
		if a.input.Err() == io.EOF {
			break
		}
	}
	if answer == "" {
		return defaultAnswer
	}
	return answer
}

func contains(needle string, haystack []string) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}

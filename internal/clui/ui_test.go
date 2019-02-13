package clui_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"testing/quick"

	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/mocks"
	"github.com/golang/mock/gomock"
)

func TestBasicUI_implements(t *testing.T) {
	var _ clui.UI = new(clui.BasicUI)
}

func TestBasicUI_Ask(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewBasicUI(inReader, writer)
		)

		go fmt.Fprintf(inWriter, "%s\nbaz\n", name)

		result, err := ui.Ask("Name?")
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := "Name? ", writer.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
			return false
		}

		return result == name
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestBasicUI_AskSecret(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewBasicUI(inReader, writer)
		)

		go fmt.Fprintf(inWriter, "%s\nbaz\n", name)

		result, err := ui.AskSecret("Name?")
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := "Name? ", writer.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
			return false
		}

		return result == name
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestBasicUI_Error(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewBasicUI(inReader, writer)
		)

		ui.Error(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestBasicUI_Error_ErrorWriter(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewBasicUI(inReader, nil)
		)

		ui.ErrorWriter = writer

		ui.Error(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestBasicUI_Output(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewBasicUI(inReader, writer)
		)

		ui.Output(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestBasicUI_Info(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewBasicUI(inReader, writer)
		)

		ui.Info(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestBasicUI_Warn(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewBasicUI(inReader, writer)
		)

		ui.Warn(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestUIWriter_implements(t *testing.T) {
	var _ io.Writer = new(clui.UIWriter)
}

func TestUIWriter(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fn := func(name0, name1 string) bool {
		ui := mocks.NewMockUI(ctrl)

		ui.EXPECT().Info(name0)
		ui.EXPECT().Info(name1)

		w := &clui.UIWriter{
			UI: ui,
		}

		fmt.Fprintln(w, name0)
		fmt.Fprintln(w, name1)

		return true
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestUIWriter_Empty(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ui := mocks.NewMockUI(ctrl)

	ui.EXPECT().Info("")

	w := &clui.UIWriter{
		UI: ui,
	}

	fmt.Fprintln(w, "")
}

func TestColorUI_implements(t *testing.T) {
	var _ clui.UI = new(clui.ColorUI)
}

func TestColorUI_Ask(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewColorUI(clui.NewBasicUI(inReader, writer))
		)

		go fmt.Fprintf(inWriter, "%s\nbaz\n", name)

		result, err := ui.Ask("Name?")
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := "Name? ", writer.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
			return false
		}

		return result == name
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestColorUI_AskSecret(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewColorUI(clui.NewBasicUI(inReader, writer))
		)

		go fmt.Fprintf(inWriter, "%s\nbaz\n", name)

		result, err := ui.AskSecret("Name?")
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := "Name? ", writer.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
			return false
		}

		return result == name
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestColorUI_Error(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewColorUI(clui.NewBasicUI(inReader, writer))
		)

		ui.Error(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestColorUI_Error_ErrorWriter(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewBasicUI(inReader, nil)
		)

		ui.ErrorWriter = writer

		ui.Error(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestColorUI_Output(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewColorUI(clui.NewBasicUI(inReader, writer))
		)

		ui.Output(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestColorUI_Info(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewColorUI(clui.NewBasicUI(inReader, writer))
		)

		ui.Info(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

func TestColorUI_Warn(t *testing.T) {
	t.Parallel()

	fn := func(name string) bool {
		inReader, inWriter := io.Pipe()
		defer func() { inReader.Close(); inWriter.Close() }()

		var (
			writer = new(bytes.Buffer)
			ui     = clui.NewColorUI(clui.NewBasicUI(inReader, writer))
		)

		ui.Warn(name)

		return writer.String() == fmt.Sprintf("%s\n", name)
	}
	if err := quick.Check(fn, nil); err != nil {
		t.Error(err)
	}
}

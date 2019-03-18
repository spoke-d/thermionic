package clui_test

import (
	"flag"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/clui"
	"github.com/spoke-d/thermionic/internal/clui/args"
	"github.com/spoke-d/thermionic/internal/clui/flagset"
	"github.com/spoke-d/thermionic/internal/clui/mocks"
)

func TestAutoComplete_Predict_Command(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		reg          = clui.NewRegistry()
		autoComplete = clui.NewAutoComplete(
			clui.NewBasicUI(os.Stdin, ioutil.Discard),
			mocks.NewMockAutoCompleteInstaller(ctrl),
			reg,
		)
	)

	if err := reg.Add("example sub", mocks.NewMockCommand(ctrl)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Add("example subs", mocks.NewMockCommand(ctrl)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Add("example dubs", mocks.NewMockCommand(ctrl)); err != nil {
		t.Fatal(err)
	}

	var (
		want = []string{"sub", "subs"}
		got  = autoComplete.Predict(args.New("clui example --foo su"))
	)
	if expected, actual := want, got; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %#v, actual: %#v", expected, actual)
	}
}

func TestAutoComplete_Predict_Flags(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		reg          = clui.NewRegistry()
		autoComplete = clui.NewAutoComplete(
			clui.NewBasicUI(os.Stdin, ioutil.Discard),
			mocks.NewMockAutoCompleteInstaller(ctrl),
			reg,
		)

		cmd0 = mocks.NewMockCommand(ctrl)
		cmd1 = mocks.NewMockCommand(ctrl)

		flagset0 = flagset.NewFlagSet("clui", flag.ExitOnError)
	)

	flagset0.String("foo", "xxx", "yyy")
	flagset0.String("bar", "xxx", "yyy")

	cmd0.EXPECT().FlagSet().Return(flagset0)

	if err := reg.Add("example sub", cmd0); err != nil {
		t.Fatal(err)
	}
	if err := reg.Add("example subs", cmd1); err != nil {
		t.Fatal(err)
	}
	if err := reg.Add("example dubs", mocks.NewMockCommand(ctrl)); err != nil {
		t.Fatal(err)
	}

	var (
		want = []string{"-bar", "-foo"}
		got  = autoComplete.Predict(args.New("clui example sub --f"))
	)
	if expected, actual := want, got; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %#v, actual: %#v", expected, actual)
	}
}

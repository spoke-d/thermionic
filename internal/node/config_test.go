package node_test

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"

	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/node/mocks"
	"github.com/golang/mock/gomock"
)

func TestConfigLoad(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	mockTx.EXPECT().Config().Return(map[string]string{
		"core.https_address": "[::]:8124",
	}, nil)

	config, err := node.ConfigLoad(mockTx, node.ConfigSchema)
	if err != nil {
		t.Errorf("expected err to be nil")
	}

	address, err := config.HTTPSAddress()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := "[::]:8124", address; expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestConfigLoadWithConfigFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	mockTx.EXPECT().Config().Return(map[string]string{
		"core.https_address": "[::]:8124",
	}, errors.New("bad"))

	_, err := node.ConfigLoad(mockTx, node.ConfigSchema)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestConfigLoadWithNewConfigFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	mockTx.EXPECT().Config().Return(map[string]string{
		"some.random.thing": "xxx",
	}, nil)

	_, err := node.ConfigLoad(mockTx, node.ConfigSchema)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestConfigPatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	mockTx.EXPECT().Config().Return(map[string]string{
		"core.https_address": "[::]:8124",
	}, nil)
	mockTx.EXPECT().UpdateConfig(map[string]string{
		"core.https_address": "[::]:8432",
	}).Return(nil)

	config, err := node.ConfigLoad(mockTx, node.ConfigSchema)
	if err != nil {
		t.Errorf("expected err to be nil")
	}

	changed, err := config.Patch(map[string]interface{}{
		"core.https_address": "[::]:8432",
	})
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := map[string]string{
		"core.https_address": "[::]:8432",
	}, changed; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	address, err := config.HTTPSAddress()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := "[::]:8432", address; expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestConfigPatchWithUpdateConfigFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	mockTx.EXPECT().Config().Return(map[string]string{
		"core.https_address": "[::]:8124",
	}, nil)
	mockTx.EXPECT().UpdateConfig(map[string]string{
		"core.https_address": "[::]:8432",
	}).Return(errors.New("bad"))

	config, err := node.ConfigLoad(mockTx, node.ConfigSchema)
	if err != nil {
		t.Errorf("expected err to be nil")
	}

	_, err = config.Patch(map[string]interface{}{
		"core.https_address": "[::]:8432",
	})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

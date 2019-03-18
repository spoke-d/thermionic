package clui_test

import (
	"testing"
	"testing/quick"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/clui"
)

func TestRegistryAdd(t *testing.T) {
	t.Parallel()

	t.Run("add", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fn := guard(func(key string) bool {
			reg := clui.NewRegistry()
			return reg.Add(key, nil) == nil
		})
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestRegistryRemove(t *testing.T) {
	t.Parallel()

	t.Run("remove with add", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fn := guard(func(key string) bool {
			reg := clui.NewRegistry()
			if err := reg.Add(key, nil); err != nil {
				t.Fatal(err)
			}
			_, err := reg.Remove(key)
			return err == nil
		})
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("remove without add", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fn := guard(func(key string) bool {
			reg := clui.NewRegistry()
			_, err := reg.Remove(key)
			return err != nil
		})
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

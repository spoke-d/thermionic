package install

import (
	"fmt"
	"os"
	"os/user"
	"testing"

	"github.com/spoke-d/thermionic/internal/fsys"
)

func TestInstaller(t *testing.T) {
	t.Parallel()

	fs := fsys.NewVirtualFileSystem()

	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.MkdirAll(u.HomeDir, 0755); err != nil {
		t.Fatal(err)
	}

	in, err := New(fs)
	if err != nil {
		t.Fatal(err)
	}

	if err := in.Install("xxx"); err != nil {
		t.Error(err)
	}
	fs.Walk(u.HomeDir, func(path string, info os.FileInfo, err error) error {
		fmt.Println(path, info, err)
		return nil
	})
}

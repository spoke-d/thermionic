package install

import (
	"fmt"

	"github.com/spoke-d/thermionic/internal/fsys"
)

type shell struct {
	fsys  fsys.FileSystem
	files []string
	cmdFn func(string, string) string
}

func (b *shell) Install(cmd, bin string) error {
	c := b.cmdFn(cmd, bin)
	for _, f := range b.files {
		if fileContains(b.fsys, f, c) {
			return fmt.Errorf("file already contains line: %q", f)
		}
		if err := appendToFile(b.fsys, f, c); err != nil {
			return err
		}
	}
	return nil
}

func (b *shell) Uninstall(cmd, bin string) error {
	c := b.cmdFn(cmd, bin)
	for _, f := range b.files {
		if !fileContains(b.fsys, f, c) {
			continue
		}

		if err := removeFromFile(b.fsys, f, c); err != nil {
			return err
		}
	}
	return nil
}

func newBash(fsys fsys.FileSystem) *shell {
	var files []string

	for _, rc := range []string{
		".bashrc",
		".bash_profile",
		".bash_login",
		".profile",
	} {
		if path, ok := filePath(fsys, rc); ok {
			files = append(files, path)
		}
	}

	return &shell{
		fsys:  fsys,
		files: files,
		cmdFn: func(cmd, bin string) string {
			return fmt.Sprintf("complete -C %s %s", bin, cmd)
		},
	}
}

func newZsh(fsys fsys.FileSystem) *shell {
	var files []string

	for _, rc := range []string{
		".zshrc",
	} {
		if path, ok := filePath(fsys, rc); ok {
			files = append(files, path)
		}
	}

	return &shell{
		fsys:  fsys,
		files: files,
		cmdFn: func(cmd, bin string) string {
			return fmt.Sprintf("complete -o nospace -C %s %s", bin, cmd)
		},
	}
}

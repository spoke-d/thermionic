package fsys_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/spoke-d/thermionic/internal/fsys"
)

func TestLocalFileSystem(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fs := fsys.NewLocalFileSystem(false)
		testFileSystemCreate(fs, dir, t)
	})

	t.Run("open", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fs := fsys.NewLocalFileSystem(false)
		testFileSystemOpen(fs, dir, t)
	})

	t.Run("rename", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fs := fsys.NewLocalFileSystem(false)
		testFileSystemRename(fs, dir, t)
	})

	t.Run("exists", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fs := fsys.NewLocalFileSystem(false)
		testFileSystemExists(fs, dir, t)
	})

	t.Run("remove", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fs := fsys.NewLocalFileSystem(false)
		testFileSystemRemove(fs, dir, t)
	})

	t.Run("walk", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fs := fsys.NewLocalFileSystem(false)
		testFileSystemWalk(fs, dir, t)
	})
}

func TestLocalFile(t *testing.T) {
	t.Parallel()

	t.Run("name", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fs := fsys.NewLocalFileSystem(false)
		testFileName(fs, dir, t)
	})

	t.Run("size", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fs := fsys.NewLocalFileSystem(false)
		testFileSize(fs, dir, t)
	})

	t.Run("read and write", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "tmpdir")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		fs := fsys.NewLocalFileSystem(false)
		testFileReadWrite(fs, dir, t)
	})
}

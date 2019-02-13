package fsys_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"

	"github.com/spoke-d/thermionic/internal/fsys"
)

func TestVirtualFileSystem(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fs := fsys.NewVirtualFileSystem()
		testFileSystemCreate(fs, dir, t)
	})

	t.Run("open", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fs := fsys.NewVirtualFileSystem()
		testFileSystemOpen(fs, dir, t)
	})

	t.Run("open with failure", func(t *testing.T) {
		var (
			dir          = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fs           = fsys.NewVirtualFileSystem()
			path         = filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
			tmpfile, err = fs.Create(path)
		)
		if err != nil {
			t.Error(err)
		}

		fsys.DeleteVirtualFileSystemFiles(fs, path)

		content := make([]byte, rand.Intn(1000)+100)
		if _, err = rand.Read(content); err != nil {
			t.Fatal(err)
		}
		if _, err = tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}

		defer fs.Remove(tmpfile.Name())
		if _, err = tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err = tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		_, err = fs.Open(path)
		if expected, actual := true, fsys.ErrNotFound(err); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("rename", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fs := fsys.NewVirtualFileSystem()
		testFileSystemRename(fs, dir, t)
	})

	t.Run("rename with failure", func(t *testing.T) {
		var (
			dir          = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fs           = fsys.NewVirtualFileSystem()
			oldPath      = filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
			newPath      = filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
			tmpfile, err = fs.Create(oldPath)
		)
		if err != nil {
			t.Error(err)
		}

		fsys.DeleteVirtualFileSystemFiles(fs, oldPath)

		content := make([]byte, rand.Intn(1000)+100)
		if _, err = rand.Read(content); err != nil {
			t.Fatal(err)
		}
		if _, err = tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}

		defer fs.Remove(tmpfile.Name())
		if _, err = tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err = tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		err = fs.Rename(oldPath, newPath)
		if expected, actual := true, fsys.ErrNotFound(err); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("exists", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fs := fsys.NewVirtualFileSystem()
		testFileSystemExists(fs, dir, t)
	})

	t.Run("remove", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fs := fsys.NewVirtualFileSystem()
		testFileSystemRemove(fs, dir, t)
	})

	t.Run("walk", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fs := fsys.NewVirtualFileSystem()
		testFileSystemWalk(fs, dir, t)
	})

	t.Run("walk with failure", func(t *testing.T) {
		var (
			dir  = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fs   = fsys.NewVirtualFileSystem()
			path = filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
		)
		if _, err := fs.Create(path); err != nil {
			t.Error(err)
		}

		fatal := errors.New("fatal")
		err := fs.Walk(dir, func(path string, info os.FileInfo, err error) error {
			return fatal
		})

		if expected, actual := fatal, err; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("walk with path mismatch", func(t *testing.T) {
		var (
			fs  = fsys.NewVirtualFileSystem()
			dir string
		)
		for i := 0; i < 10; i++ {
			dir = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			path := filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
			if _, err := fs.Create(path); err != nil {
				t.Error(err)
			}
		}

		err := fs.Walk(dir, func(path string, info os.FileInfo, err error) error {
			return nil
		})

		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestVirtualFile(t *testing.T) {
	t.Parallel()

	t.Run("name", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fs := fsys.NewVirtualFileSystem()
		testFileName(fs, dir, t)
	})

	t.Run("size", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fs := fsys.NewVirtualFileSystem()
		testFileSize(fs, dir, t)
	})

	t.Run("read and write", func(t *testing.T) {
		dir := fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
		fs := fsys.NewVirtualFileSystem()
		testFileReadWrite(fs, dir, t)
	})

	t.Run("sync", func(t *testing.T) {
		var (
			fs       = fsys.NewVirtualFileSystem()
			dir      = fmt.Sprintf("tmpdir-%d", rand.Intn(1000))
			fileName = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
			path     = filepath.Join(dir, fileName)
		)
		file, err := fs.Create(path)
		if err != nil {
			t.Error(err)
		}

		defer file.Close()

		res := file.Sync()
		if expected, actual := true, res == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

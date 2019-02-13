package fsys_test

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/pkg/errors"
)

func TestBuildingFileSystem(t *testing.T) {
	t.Parallel()

	t.Run("build", func(t *testing.T) {
		fn := func(name string) bool {
			_, err := fsys.Build(
				fsys.With(name),
				fsys.WithMMAP(true),
			)
			if err != nil {
				t.Fatal(err)
			}
			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid build", func(t *testing.T) {
		_, err := fsys.Build(
			func(config *fsys.Config) error {
				return errors.Errorf("bad")
			},
		)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("local", func(t *testing.T) {
		config, err := fsys.Build(
			fsys.With("local"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = fsys.New(config)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("virtual", func(t *testing.T) {
		config, err := fsys.Build(
			fsys.With("virtual"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = fsys.New(config)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		config, err := fsys.Build(
			fsys.With("invalid"),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = fsys.New(config)
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestNotFound(t *testing.T) {
	t.Parallel()

	t.Run("source", func(t *testing.T) {
		fn := func(source string) bool {
			err := fsys.NotFound(errors.New(source))

			if expected, actual := source, err.Error(); expected != actual {
				t.Errorf("expected: %q, actual: %q", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		fn := func(source string) bool {
			err := fsys.NotFound(errors.New(source))

			if expected, actual := true, fsys.ErrNotFound(err); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		fn := func(source string) bool {
			err := errors.New(source)

			if expected, actual := false, fsys.ErrNotFound(err); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func testFileSystemCreate(fs fsys.FileSystem, dir string, t *testing.T) {
	path := filepath.Join(dir, "tmpfile")
	file, err := fs.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fs.Exists(path) {
		t.Errorf("expected: %q to exist", path)
	}
	if expected, actual := int64(0), file.Size(); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}

func testFileSystemOpen(fs fsys.FileSystem, dir string, t *testing.T) {
	path := filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
	tmpfile, err := fs.Create(path)
	if err != nil {
		t.Error(err)
	}

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

	file, err := fs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	buf := make([]byte, len(content))
	if _, err := io.ReadFull(file, buf); err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(content, buf) {
		t.Errorf("expected: %v, actual: %v", content, buf)
	}
}

func testFileSystemRename(fs fsys.FileSystem, dir string, t *testing.T) {
	path := filepath.Join(dir, fmt.Sprintf("tmpfile-%d", rand.Intn(1000)))
	tmpfile, err := fs.Create(path)
	if err != nil {
		t.Error(err)
	}

	content := make([]byte, rand.Intn(1000)+100)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}

	defer fs.Remove(tmpfile.Name())
	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	var (
		oldPath = tmpfile.Name()
		newPath = fmt.Sprintf("%s-new", tmpfile.Name())
	)
	if err := fs.Rename(oldPath, newPath); err != nil {
		t.Error(err)
	}

	if fs.Exists(oldPath) {
		t.Errorf("expected: %q to not exist", newPath)
	}

	if !fs.Exists(newPath) {
		t.Errorf("expected: %q to exist", newPath)
	}
}

func testFileSystemExists(fs fsys.FileSystem, dir string, t *testing.T) {
	if path := filepath.Join(dir, "tmpfile"); fs.Exists(path) {
		t.Errorf("expected: %q to not exist", path)
	}

	// exists is run in all the following
	testFileSystemOpen(fs, dir, t)
	testFileSystemCreate(fs, dir, t)
	testFileSystemRename(fs, dir, t)
	testFileSystemRemove(fs, dir, t)
}

func testFileSystemRemove(fs fsys.FileSystem, dir string, t *testing.T) {
	path := filepath.Join(dir, "tmpfile")
	file, err := fs.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fs.Exists(path) {
		t.Errorf("expected: %q to exist", path)
	}

	if err := fs.Remove(path); err != nil {
		t.Errorf("expected: %q to not exist", path)
	}
}

func testFileSystemWalk(fs fsys.FileSystem, dir string, t *testing.T) {
	contains := func(paths []string, path string) bool {
		for _, v := range paths {
			if v == path {
				return true
			}
		}
		return false
	}
	paths := make([]string, rand.Intn(100)+1)
	for k := range paths {
		path := filepath.Join(dir, fmt.Sprintf("tmpfile-%d", k))
		file, err := fs.Create(path)
		if err != nil {
			t.Error(err)
		}

		defer file.Close()

		if !fs.Exists(file.Name()) {
			t.Errorf("expected: %q to exist", file.Name())
		}

		paths[k] = path
	}

	if err := fs.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info == nil {
			t.Errorf("expected: %q file info to exist", path)
		}

		if info.IsDir() {
			return nil
		}

		filepath := filepath.Join(dir, info.Name())
		if !contains(paths, filepath) {
			t.Errorf("expected: %q to exist", filepath)
		}

		if expected, actual := int64(0), info.Size(); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		return err
	}); err != nil {
		t.Error(err)
	}
}

func testFileName(fs fsys.FileSystem, dir string, t *testing.T) {
	var (
		fileName = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
		path     = filepath.Join(dir, fileName)
	)
	file, err := fs.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fs.Exists(path) {
		t.Errorf("expected: %q to exist", path)
	}

	if expected, actual := path, file.Name(); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}

func testFileSize(fs fsys.FileSystem, dir string, t *testing.T) {
	var (
		fileName = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
		path     = filepath.Join(dir, fileName)
	)
	file, err := fs.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fs.Exists(path) {
		t.Errorf("expected: %q to exist", path)
	}
	if expected, actual := int64(0), file.Size(); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}

	content := make([]byte, rand.Intn(1000)+100)
	if _, err := rand.Read(content); err != nil {
		t.Error(err)
	}
	if _, err := file.Write(content); err != nil {
		t.Error(err)
	}

	if expected, actual := file.Size(), int64(len(content)); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}

func testFileReadWrite(fs fsys.FileSystem, dir string, t *testing.T) {
	var (
		fileName = fmt.Sprintf("tmpfile-%d", rand.Intn(1000))
		path     = filepath.Join(dir, fileName)
	)
	file, err := fs.Create(path)
	if err != nil {
		t.Error(err)
	}

	defer file.Close()

	if !fs.Exists(path) {
		t.Errorf("expected: %q to exist", path)
	}

	var (
		n     int
		bytes []byte
	)
	if n, err = file.Read(bytes); err != nil {
		t.Error(err)
	} else if n != 0 {
		t.Errorf("expected: %q to be 0", n)
	}

	content := make([]byte, rand.Intn(1000)+100)
	if _, err = rand.Read(content); err != nil {
		t.Error(err)
	}
	encoded := base64.RawStdEncoding.EncodeToString(content)
	encodedBytes := []byte(encoded)
	if n, err = file.Write(encodedBytes); err != nil {
		t.Error(err)
	} else if n == 0 || n != len(encodedBytes) {
		t.Errorf("expected: %q to be %d", n, len(encodedBytes))
	}
	if err = file.Sync(); err != nil {
		t.Error(err)
	}
	if err = file.Close(); err != nil {
		t.Error(err)
	}

	// For some reason, we can't read after a write
	file, err = fs.Open(path)
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	contentBytes, err := ioutil.ReadAll(file)
	if err != nil {
		t.Error(err)
	}
	if expected, actual := encodedBytes, contentBytes; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}

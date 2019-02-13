package fsys

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spoke-d/thermionic/internal/fsys/ioext"
	"github.com/spoke-d/thermionic/internal/fsys/lock"
	"github.com/spoke-d/thermionic/internal/fsys/mmap"
	"github.com/pkg/errors"
)

const mkdirAllMode = 0755

// LocalFileSystem represents a local disk filesystem
type LocalFileSystem struct {
	mmap bool
}

// NewLocalFileSystem yields a local disk filesystem.
func NewLocalFileSystem(mmap bool) LocalFileSystem {
	return LocalFileSystem{mmap}
}

// Create takes a path, creates the file and then returns a File back that
// can be used. This returns an error if the file can not be created in
// some way.
func (LocalFileSystem) Create(path string) (File, error) {
	f, err := os.Create(path)
	return localFile{
		File:   f,
		Reader: f,
		Closer: f,
	}, errors.WithStack(err)
}

// Open takes a path, opens a potential file and then returns a File if
// that file exists, otherwise it returns an error if the file wasn't found.
func (fs LocalFileSystem) Open(path string) (File, error) {
	f, err := os.Open(path)
	return fs.open(f, err)
}

// OpenFile takes a path, opens a potential file and then returns a File if
// that file exists, otherwise it returns an error if the file wasn't found.
func (fs LocalFileSystem) OpenFile(path string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(path, flag, perm)
	return fs.open(f, err)
}

// Rename takes a current destination path and a new destination path and will
// rename the a File if it exists, otherwise it returns an error if the file
// wasn't found.
func (LocalFileSystem) Rename(oldname, newname string) error {
	err := os.Rename(oldname, newname)
	return errors.WithStack(err)
}

// Exists takes a path and checks to see if the potential file exists or
// not.
// Note: If there is an error trying to read that file, it will return false
// even if the file already exists.
func (LocalFileSystem) Exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return !os.IsNotExist(err)
}

// Remove takes a path, removes a potential file, if no file doesn't exist it
// will return not found.
func (LocalFileSystem) Remove(path string) error {
	err := os.Remove(path)
	return errors.WithStack(err)
}

// RemoveAll takes a path, removes all potential files and directories, if
// no file or directory doesn't exist it will return not found.
func (LocalFileSystem) RemoveAll(path string) error {
	err := os.RemoveAll(path)
	return errors.WithStack(err)
}

// Mkdir takes a path and generates a directory structure from that path,
// with the given file mode and if there is a failure it will return an error.
func (LocalFileSystem) Mkdir(path string, mode os.FileMode) error {
	err := os.Mkdir(path, mode)
	return errors.WithStack(err)
}

// MkdirAll takes a path and generates a directory structure from that path,
// if there is a failure it will return an error.
func (LocalFileSystem) MkdirAll(path string, mode os.FileMode) error {
	err := os.MkdirAll(path, mode)
	return errors.WithStack(err)
}

// Chtimes updates the modifier times for a given path or returns an error
// upon failure
func (LocalFileSystem) Chtimes(path string, atime, mtime time.Time) error {
	err := os.Chtimes(path, atime, mtime)
	return errors.WithStack(err)
}

// Walk over a specific directory and will return an error if there was an
// error whilst walking.
func (LocalFileSystem) Walk(root string, walkFn filepath.WalkFunc) error {
	err := filepath.Walk(root, walkFn)
	return errors.WithStack(err)
}

// Lock attempts to create a locking file for a given path.
func (LocalFileSystem) Lock(path string) (r Releaser, existed bool, err error) {
	r, existed, err = lock.New(path)
	r = deletingReleaser{path, r}
	return r, existed, errors.WithStack(err)
}

// CopyFile copies a directory recursively, overwriting the target if it
// exists.
func (fs LocalFileSystem) CopyFile(source, dest string) error {
	s, err := os.Open(source)
	if err != nil {
		return errors.WithStack(err)
	}
	defer s.Close()

	fi, err := s.Stat()
	if err != nil {
		return errors.WithStack(err)
	}

	d, err := os.Create(dest)
	if err != nil {
		if os.IsExist(err) {
			if d, err = os.OpenFile(dest, os.O_WRONLY, fi.Mode()); err != nil {
				return errors.WithStack(err)
			}
		} else {
			return errors.WithStack(err)
		}
	}
	defer d.Close()

	if _, err = io.Copy(d, s); err != nil {
		return errors.WithStack(err)
	}

	_, uid, gid := getOwnerMode(fi)
	return d.Chown(uid, gid)
}

// CopyDir copies a directory recursively, overwriting the target if it
// exists.
func (fs LocalFileSystem) CopyDir(source, dest string) error {
	// Get info about source.
	info, err := os.Stat(source)
	if err != nil {
		return errors.Wrapf(err, "failed to get source directory info")
	}

	if !info.IsDir() {
		return errors.Errorf("source is not a directory")
	}

	// Remove dest if it already exists.
	if fs.Exists(dest) {
		if err := os.RemoveAll(dest); err != nil {
			return errors.Wrapf(err, "failed to remove destination directory %s", dest)
		}
	}

	// Create dest.
	if err := os.MkdirAll(dest, info.Mode()); err != nil {
		return errors.Wrapf(err, "failed to create destination directory %s", dest)
	}

	// Copy all files.
	entries, err := ioutil.ReadDir(source)
	if err != nil {
		return errors.Wrapf(err, "failed to read source directory %s", source)
	}

	for _, entry := range entries {
		sourcePath := filepath.Join(source, entry.Name())
		destPath := filepath.Join(dest, entry.Name())

		if entry.IsDir() {
			err := fs.CopyDir(sourcePath, destPath)
			if err != nil {
				return errors.Wrapf(err, "failed to copy sub-directory from %s to %s", sourcePath, destPath)
			}
		} else {
			if err := copyFile(sourcePath, destPath); err != nil {
				return errors.Wrapf(err, "failed to copy file from %s to %s", sourcePath, destPath)
			}
		}
	}

	return nil
}

// Symlink creates newname as a symbolic link to oldname.
// If there is an error, it will be of type *LinkError.
func (LocalFileSystem) Symlink(oldname, newname string) error {
	return os.Symlink(oldname, newname)
}

func (fs LocalFileSystem) open(f *os.File, err error) (File, error) {
	if err != nil {
		if err == os.ErrNotExist {
			return nil, errNotFound{err}
		}
		return nil, errors.WithStack(err)
	}

	local := localFile{
		File:   f,
		Reader: f,
		Closer: f,
	}

	if fs.mmap {
		r, err := mmap.New(f)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		local.Reader = ioext.OffsetReader(r, 0)
		local.Closer = multiCloser{r, f}
	}

	return local, nil
}

type localFile struct {
	*os.File
	io.Reader
	io.Closer
}

func (f localFile) Read(p []byte) (int, error) {
	d, err := f.Reader.Read(p)
	return d, err
}

func (f localFile) Close() error {
	err := f.Closer.Close()
	return err
}

func (f localFile) Size() int64 {
	fi, err := f.File.Stat()
	if err != nil {
		panic(err)
	}
	return fi.Size()
}

type deletingReleaser struct {
	path string
	r    Releaser
}

func (dr deletingReleaser) Release() error {
	// Remove before Release should be safe, and prevents a race.
	if err := os.Remove(dr.path); err != nil {
		return errors.WithStack(err)
	}
	err := dr.r.Release()
	return errors.WithStack(err)
}

// copyFile copies a file, overwriting the target if it exists.
func copyFile(source, dest string) error {
	s, err := os.Open(source)
	if err != nil {
		return errors.WithStack(err)
	}
	defer s.Close()

	fi, err := s.Stat()
	if err != nil {
		return errors.WithStack(err)
	}

	d, err := os.Create(dest)
	if err != nil {
		if os.IsExist(err) {
			d, err = os.OpenFile(dest, os.O_WRONLY, fi.Mode())
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	defer d.Close()

	if _, err := io.Copy(d, s); err != nil {
		return errors.WithStack(err)
	}

	_, uid, gid := getOwnerMode(fi)
	err = d.Chown(uid, gid)
	return errors.WithStack(err)
}

func getOwnerMode(fInfo os.FileInfo) (os.FileMode, int, int) {
	mode := fInfo.Mode()
	uid := int(fInfo.Sys().(*syscall.Stat_t).Uid)
	gid := int(fInfo.Sys().(*syscall.Stat_t).Gid)
	return mode, uid, gid
}

// multiCloser closes all underlying io.Closers.
// If an error is encountered, closings continue.
type multiCloser []io.Closer

func (c multiCloser) Close() error {
	var errs []error
	for _, closer := range c {
		if closer == nil {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return multiCloseError(errs)
	}
	return nil
}

type multiCloseError []error

func (e multiCloseError) Error() string {
	a := make([]string, len(e))
	for i, err := range e {
		a[i] = err.Error()
	}
	return strings.Join(a, "; ")
}

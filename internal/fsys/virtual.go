package fsys

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// VirtualFileSystem represents an in-memory filesystem.
type VirtualFileSystem struct {
	mutex sync.RWMutex
	files map[string]*virtualFile
}

// NewVirtualFileSystem yields an in-memory filesystem.
func NewVirtualFileSystem() *VirtualFileSystem {
	return &VirtualFileSystem{
		files: map[string]*virtualFile{},
	}
}

// Create takes a path, creates the file and then returns a File back that
// can be used. This returns an error if the file can not be created in
// some way.
func (fs *VirtualFileSystem) Create(path string) (File, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	// os.Create truncates any existing file. So we do, too.
	f := &virtualFile{
		name:  path,
		atime: time.Now(),
		mtime: time.Now(),
	}
	fs.files[path] = f

	return f, nil
}

// Open takes a path, opens a potential file and then returns a File if
// that file exists, otherwise it returns an error if the file wasn't found.
func (fs *VirtualFileSystem) Open(path string) (File, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	f, ok := fs.files[path]
	if !ok {
		return nil, errNotFound{os.ErrNotExist}
	}

	return &virtualFile{
		name:  f.name,
		atime: f.atime,
		mtime: f.mtime,
		buf:   *bytes.NewBuffer(f.buf.Bytes()),
	}, nil
}

// OpenFile takes a path, opens a potential file and then returns a File if
// that file exists, otherwise it returns an error if the file wasn't found.
func (fs *VirtualFileSystem) OpenFile(path string, flag int, perm os.FileMode) (File, error) {
	return fs.Open(path)
}

// Rename takes a current destination path and a new destination path and will
// rename the a File if it exists, otherwise it returns an error if the file
// wasn't found.
func (fs *VirtualFileSystem) Rename(oldname, newname string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	f, ok := fs.files[oldname]
	if !ok {
		return errNotFound{os.ErrNotExist}
	}

	// potentially destructive to newname!
	delete(fs.files, oldname)
	fs.files[newname] = f

	return nil
}

// Exists takes a path and checks to see if the potential file exists or
// not.
// Note: If there is an error trying to read that file, it will return false
// even if the file already exists.
func (fs *VirtualFileSystem) Exists(path string) bool {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	_, ok := fs.files[path]
	return ok
}

// Remove takes a path, removes a potential file, if no file doesn't exist it
// will return not found.
func (fs *VirtualFileSystem) Remove(path string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if _, ok := fs.files[path]; !ok {
		return errNotFound{os.ErrNotExist}
	}

	delete(fs.files, path)
	return nil
}

// RemoveAll takes a path, removes a potential file, if no file doesn't exist it
// will return not found.
func (fs *VirtualFileSystem) RemoveAll(path string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	paths := make(map[string]struct{})
	for k := range fs.files {
		if strings.HasPrefix(k, path) {
			paths[k] = struct{}{}
		}
	}
	if len(paths) == 0 {
		return errNotFound{os.ErrNotExist}
	}

	for k := range paths {
		delete(fs.files, k)
	}
	return nil
}

// Mkdir takes a path and generates a directory structure from that path,
// with the given file mode and if there is a failure it will return an error.
func (fs *VirtualFileSystem) Mkdir(path string, mode os.FileMode) error {
	return nil
}

// MkdirAll takes a path and generates a directory structure from that path,
// if there is a failure it will return an error.
func (fs *VirtualFileSystem) MkdirAll(path string, mode os.FileMode) error {
	return nil
}

// Chtimes updates the modifier times for a given path or returns an error
// upon failure
func (fs *VirtualFileSystem) Chtimes(path string, atime, mtime time.Time) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	f, ok := fs.files[path]
	if !ok {
		return errNotFound{os.ErrNotExist}
	}

	f.atime, f.mtime = atime, mtime

	return nil
}

// Walk over a specific directory and will return an error if there was an
// error whilst walking.
func (fs *VirtualFileSystem) Walk(root string, walkFn filepath.WalkFunc) error {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	for path, f := range fs.files {
		if !strings.HasPrefix(path, root) {
			continue
		}

		if err := walkFn(path, virtualFileInfo{
			name:  filepath.Base(f.name),
			size:  int64(f.buf.Len()),
			mtime: f.mtime,
		}, nil); err != nil {
			return err
		}
	}
	return nil
}

// Lock attempts to create a locking file for a given path.
func (fs *VirtualFileSystem) Lock(path string) (r Releaser, existed bool, err error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	// Simulate locked as nonempty file, so we can test recovery behavior.
	if file, ok := fs.files[path]; ok {
		existed = true
		if file.Size() > 0 {
			return nil, existed, errors.Errorf("%s already exists and is locked", path)
		}
	}

	// Copy/paste.
	fs.files[path] = &virtualFile{
		name:  path,
		atime: time.Now(),
		mtime: time.Now(),
	}
	fs.files[path].buf.WriteString("locked!")
	return virtualReleaser(func() error { return fs.Remove(path) }), existed, nil
}

// CopyFile copies a directory recursively, overwriting the target if it
// exists.
func (fs *VirtualFileSystem) CopyFile(source, dest string) error {
	f, ok := fs.files[source]
	if !ok {
		return os.ErrNotExist
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, &f.buf); err != nil {
		return errors.WithStack(err)
	}

	fs.files[dest] = &virtualFile{
		name:  dest,
		buf:   *buf,
		atime: f.atime,
		mtime: f.mtime,
	}
	return nil
}

// CopyDir copies a directory recursively, overwriting the target if it
// exists.
func (fs *VirtualFileSystem) CopyDir(source, dest string) error {
	for k := range fs.files {
		if strings.HasPrefix(k, source) {
			prefix := strings.TrimPrefix(k, source)

			if err := fs.CopyFile(k, filepath.Join(dest, prefix)); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

// Symlink creates newname as a symbolic link to oldname.
// If there is an error, it will be of type *LinkError.
func (fs *VirtualFileSystem) Symlink(oldname, newname string) error {
	return fs.CopyFile(oldname, newname)
}

type virtualFile struct {
	name  string
	mutex sync.Mutex
	buf   bytes.Buffer
	atime time.Time
	mtime time.Time
}

func (f *virtualFile) Read(p []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.buf.Read(p)
}

func (f *virtualFile) Write(p []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.buf.Write(p)
}

func (f *virtualFile) Close() error { return nil }
func (f *virtualFile) Name() string { return f.name }

func (f *virtualFile) Size() int64 {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return int64(f.buf.Len())
}

func (f *virtualFile) Sync() error { return nil }

type virtualFileInfo struct {
	name  string
	size  int64
	mtime time.Time
}

func (fi virtualFileInfo) Name() string       { return fi.name }
func (fi virtualFileInfo) Size() int64        { return fi.size }
func (fi virtualFileInfo) Mode() os.FileMode  { return os.FileMode(0644) }
func (fi virtualFileInfo) ModTime() time.Time { return fi.mtime }
func (fi virtualFileInfo) IsDir() bool        { return false }
func (fi virtualFileInfo) Sys() interface{}   { return nil }

type virtualReleaser func() error

func (r virtualReleaser) Release() error { return r() }

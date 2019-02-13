// +build darwin linux

package mmap

import (
	"io"
	"os"
	"runtime"
	"syscall"

	"github.com/pkg/errors"
)

// readerAt reads a memory-mapped file.
//
// Like any io.ReaderAt, clients can execute parallel ReadAt calls, but it is
// not safe to call Close and reading methods concurrently.
type readerAt struct {
	data []byte
}

// New memory-maps the given file for reading.
func newReaderAt(f *os.File) (ReaderAt, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := fi.Size()
	if size == 0 {
		return nil, nil
	}
	if size < 0 {
		return nil, errors.Errorf("mmap: file %q has negative size", f.Name())
	}
	if size != int64(int(size)) {
		return nil, errors.Errorf("mmap: file %q is too large", f.Name())
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	r := &readerAt{data}
	runtime.SetFinalizer(r, (*readerAt).Close)
	return r, nil
}

// At returns the byte at index i.
func (r *readerAt) At(i int) byte {
	return r.data[i]
}

// ReadAt implements the io.ReaderAt interface.
func (r *readerAt) ReadAt(p []byte, off int64) (int, error) {
	if r.data == nil {
		return 0, errors.New("mmap: closed")
	}
	if off < 0 || int64(len(r.data)) < off {
		return 0, errors.Errorf("mmap: invalid ReadAt offset %d", off)
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Len returns the length of the underlying memory-mapped file.
func (r *readerAt) Len() int {
	return len(r.data)
}

// Close closes the reader.
func (r *readerAt) Close() error {
	if r.data == nil {
		return nil
	}
	data := r.data
	r.data = nil
	runtime.SetFinalizer(r, nil)
	return syscall.Munmap(data)
}

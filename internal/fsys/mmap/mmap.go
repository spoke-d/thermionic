package mmap

import "os"

// ReaderAt reads a memory-mapped file.
type ReaderAt interface {

	// ReadAt implements the io.ReaderAt interface.
	ReadAt([]byte, int64) (int, error)

	// At returns the byte at index i.
	At(i int) byte

	// Len returns the length of the underlying memory-mapped file.
	Len() int

	// Close closes the reader.
	Close() error
}

// New memory-maps the given file for reading.
func New(f *os.File) (ReaderAt, error) {
	return newReaderAt(f)
}

// Open memory-maps the named file for reading.
func Open(filename string) (ReaderAt, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return New(f)
}

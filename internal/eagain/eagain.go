package eagain

import (
	"io"
	"os"
	"syscall"
)

// Reader represents an io.Reader that handles EAGAIN
type Reader struct {
	reader io.Reader
}

// NewReader creates a Reader from an io.Reader
func NewReader(reader io.Reader) Reader {
	return Reader{
		reader: reader,
	}
}

// Read behaves like io.Reader.Read but will retry on EAGAIN
func (er Reader) Read(p []byte) (int, error) {
again:
	n, err := er.reader.Read(p)
	if err == nil {
		return n, nil
	}

	// keep retrying on EAGAIN
	ok, errno := getErrno(err)
	if ok && (errno == syscall.EAGAIN || errno == syscall.EINTR) {
		goto again
	}

	return n, err
}

// Writer represents an io.Writer that handles EAGAIN
type Writer struct {
	writer io.Writer
}

// NewWriter creates a Writer from an io.Writer
func NewWriter(writer io.Writer) Writer {
	return Writer{
		writer: writer,
	}
}

// Write behaves like io.Writer.Write but will retry on EAGAIN
func (ew Writer) Write(p []byte) (int, error) {
again:
	n, err := ew.writer.Write(p)
	if err == nil {
		return n, nil
	}

	// keep retrying on EAGAIN
	ok, errno := getErrno(err)
	if ok && (errno == syscall.EAGAIN || errno == syscall.EINTR) {
		goto again
	}

	return n, err
}

func getErrno(err error) (iserrno bool, errno error) {
	if sysErr, ok := err.(*os.SyscallError); ok {
		return true, sysErr.Err
	}

	if pathErr, ok := err.(*os.PathError); ok {
		return true, pathErr.Err
	}

	if tmpErrno, ok := err.(syscall.Errno); ok {
		return true, tmpErrno
	}

	return false, nil
}

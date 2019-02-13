package install

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/spoke-d/thermionic/internal/fsys"
)

type installer interface {
	Install(string, string) error
	Uninstall(string, string) error
}

// ExecutableFn is used to return the current binary path, so that we know
// which application to call when inserting the completion command.
type ExecutableFn func() (string, error)

// Installer represents a series of different shell installers. Calling
// Install and Uninstall to add and remove the correct shell installer commands
// retrospectively.
type Installer struct {
	installers []installer
	binaryPath string
}

// New creates a new installer with the correct dependencies and sane
// defaults.
// Returns an error if it can't locate the correct binary path.
func New(fsys fsys.FileSystem) (*Installer, error) {
	path, err := getBinaryPath(os.Executable)
	if err != nil {
		return nil, err
	}

	in := &Installer{
		installers: []installer{
			newBash(fsys),
			newZsh(fsys),
		},
		binaryPath: path,
	}
	return in, nil
}

// Install takes a command argument and installs the command for the shell
func (i *Installer) Install(cmd string) error {
	for _, in := range i.installers {
		if err := in.Install(cmd, i.binaryPath); err != nil {
			return err
		}
	}
	return nil
}

// Uninstall removes the auto-complete installer shell
func (i *Installer) Uninstall(cmd string) error {
	for _, in := range i.installers {
		if err := in.Uninstall(cmd, i.binaryPath); err != nil {
			return err
		}
	}
	return nil
}

// NopInstaller can be used as a default nop installer.
type NopInstaller struct{}

// NewNop creates an installer that performs no operations
func NewNop() *NopInstaller {
	return &NopInstaller{}
}

// Install takes a command argument and installs the command for the shell
func (*NopInstaller) Install(cmd string) error { return nil }

// Uninstall removes the auto-complete installer shell
func (*NopInstaller) Uninstall(cmd string) error { return nil }

func getBinaryPath(fn ExecutableFn) (string, error) {
	path, err := fn()
	if err != nil {
		return "", err
	}
	return filepath.Abs(path)
}

func filePath(fsys fsys.FileSystem, file string) (string, bool) {
	u, err := user.Current()
	if err != nil {
		return "", false
	}

	path := filepath.Join(u.HomeDir, file)
	return path, path != "" && fsys.Exists(path)
}

func fileContains(fsys fsys.FileSystem, name, pattern string) bool {
	f, err := fsys.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), pattern) {
			return true
		}
	}
	return false
}

func appendToFile(fsys fsys.FileSystem, name, content string) error {
	f, err := fsys.OpenFile(name, os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return err
	}

	defer f.Close()

	_, err = f.Write([]byte(fmt.Sprintf("\n%s\n", content)))
	return err
}

func removeFromFile(fsys fsys.FileSystem, name, content string) error {
	backupName := fmt.Sprintf("%s.bck", name)
	if err := copyFile(fsys, name, backupName); err != nil {
		return err
	}

	tmp, err := removeContentFromTmpFile(fsys, name, content)
	if err != nil {
		return err
	}

	if err := copyFile(fsys, tmp, name); err != nil {
		return err
	}

	return os.Remove(backupName)
}

func copyFile(fsys fsys.FileSystem, src, dst string) error {
	in, err := fsys.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := fsys.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func removeContentFromTmpFile(fsys fsys.FileSystem, name, content string) (string, error) {
	file, err := fsys.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()

	tmpFile, err := ioutil.TempFile("/tmp", "complete-")
	if err != nil {
		return "", err
	}
	defer tmpFile.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, content) {
			continue
		}
		if _, err := tmpFile.WriteString(fmt.Sprintf("%s\n", line)); err != nil {
			return "", err
		}
	}

	return tmpFile.Name(), nil
}

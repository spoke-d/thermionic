package sys

import (
	"net"
	"os"
	"os/user"
	"path/filepath"

	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/pkg/errors"
)

// OS is a high-level facade for accessing all operating-system
// level functionality that therm uses.
type OS struct {
	varDir   string
	cacheDir string
	logDir   string
}

// DefaultOS returns a fresh uninitialized OS instance with default values.
func DefaultOS() *OS {
	return &OS{
		varDir:   VarPath(),
		cacheDir: CachePath(),
		logDir:   LogPath(),
	}
}

func New(varDir, cacheDir, logDir string) *OS {
	return &OS{
		varDir:   varDir,
		cacheDir: cacheDir,
		logDir:   logDir,
	}
}

// Init our internal data structures.
func (s *OS) Init(fileSystem fsys.FileSystem) error {
	err := s.initDirs(fileSystem)
	return errors.WithStack(err)
}

// Make sure all our directories are available.
func (s *OS) initDirs(fileSystem fsys.FileSystem) error {
	dirs := []struct {
		path string
		mode os.FileMode
	}{
		{s.varDir, 0711},
		{s.cacheDir, 0700},
		{s.logDir, 0700},
		{filepath.Join(s.varDir, "database"), 0700},
	}

	for _, dir := range dirs {
		err := fileSystem.Mkdir(dir.path, dir.mode)
		if err != nil && !os.IsExist(errors.Cause(err)) {
			return errors.Wrapf(err, "failed to init dir %s", dir.path)
		}
	}

	return nil
}

// LocalDatabasePath returns the path of the local database file.
func (s *OS) LocalDatabasePath() string {
	return filepath.Join(s.varDir, "database", "local.db")
}

// GlobalDatabaseDir returns the path of the global database directory.
func (s *OS) GlobalDatabaseDir() string {
	return filepath.Join(s.varDir, "database", "global")
}

// GlobalDatabasePath returns the path of the global database SQLite file
// managed by dqlite.
func (s *OS) GlobalDatabasePath() string {
	return filepath.Join(s.GlobalDatabaseDir(), "db.bin")
}

// LocalNoncePath returns the path of the nonce for the daemon
func (s *OS) LocalNoncePath() string {
	return filepath.Join(s.varDir, "nonce.txt")
}

// VarDir represents the Data directory (e.g. /var/lib/therm/).
func (s *OS) VarDir() string {
	return s.varDir
}

// CacheDir represents the Cache directory (e.g. /var/cache/therm/).
func (s *OS) CacheDir() string {
	return s.cacheDir
}

// LogDir represents the Log directory (e.g. /var/log/therm).
func (s *OS) LogDir() string {
	return s.logDir
}

// Hostname returns the host name reported by the kernel.
func (s *OS) Hostname() (string, error) {
	return os.Hostname()
}

// HostNames will generate a list of names for which the certificate will be
// valid.
// This will include the hostname and ip address
func (s *OS) HostNames() ([]string, error) {
	hostName, err := s.Hostname()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	result := []string{hostName}

	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for _, i := range interfaces {
		if (i.Flags & net.FlagLoopback) > 0 {
			continue
		}

		addrs, err := i.Addrs()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		for _, addr := range addrs {
			result = append(result, addr.String())
		}
	}

	return result, nil
}

// User returns the current user.
func (s *OS) User() (*user.User, error) {
	return user.Current()
}

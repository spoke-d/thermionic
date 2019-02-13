package sys

import (
	"os"
	"path/filepath"
)

// VarPath returns the provided path elements joined by a slash and
// appended to the end of $THERM_DIR, which defaults to /var/lib/therm.
func VarPath(path ...string) string {
	varDir, ok := os.LookupEnv("THERM_DIR")
	if !ok && varDir == "" {
		varDir = "/var/lib/therm"
	}

	items := []string{varDir}
	items = append(items, path...)
	return filepath.Join(items...)
}

// CachePath returns the directory that THERM should its cache under. If
// THERM_DIR is set, this path is $THERM_DIR/cache, otherwise it is
// /var/cache/therm.
func CachePath(path ...string) string {
	varDir, ok := os.LookupEnv("THERM_DIR")
	logDir := "/var/cache/therm"
	if ok && varDir != "" {
		logDir = filepath.Join(varDir, "cache")
	}
	items := []string{logDir}
	items = append(items, path...)
	return filepath.Join(items...)
}

// LogPath returns the directory that THERM should put logs under. If
// THERM_DIR is set, this path is $THERM_DIR/logs, otherwise it is
// /var/log/therm.
func LogPath(path ...string) string {
	varDir, ok := os.LookupEnv("THERM_DIR")
	logDir := "/var/log/therm"
	if ok && varDir != "" {
		logDir = filepath.Join(varDir, "logs")
	}
	items := []string{logDir}
	items = append(items, path...)
	return filepath.Join(items...)
}

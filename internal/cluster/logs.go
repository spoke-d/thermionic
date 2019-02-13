package cluster

import (
	"fmt"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// DqliteLog redirects dqlite's logs to our own logger
func DqliteLog(logger log.Logger) func(dqlite.LogLevel, string, ...interface{}) {
	return func(logLevel dqlite.LogLevel, format string, a ...interface{}) {
		format = fmt.Sprintf("%s", format)
		switch logLevel {
		case dqlite.LogDebug:
			level.Debug(logger).Log("forward-from", "dqlite", "msg", fmt.Sprintf(format, a...))
		case dqlite.LogInfo:
			level.Info(logger).Log("forward-from", "dqlite", "msg", fmt.Sprintf(format, a...))
		case dqlite.LogWarn:
			level.Warn(logger).Log("forward-from", "dqlite", "msg", fmt.Sprintf(format, a...))
		case dqlite.LogError:
			level.Error(logger).Log("forward-from", "dqlite", "msg", fmt.Sprintf(format, a...))
		}
	}
}

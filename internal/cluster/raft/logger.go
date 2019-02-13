package raft

import (
	"bytes"
	"log"
	"strings"

	gokitlog "github.com/go-kit/kit/log"
	gokitlevel "github.com/go-kit/kit/log/level"
)

// NewLogger will create a logger that can be used by the raft instance, whilst
// surfacing information to the internal logger.
func NewLogger(logger gokitlog.Logger) *log.Logger {
	return log.New(&raftLogWriter{
		logger: logger,
	}, "", 0)
}

// Implement io.Writer on top of the logging system.
type raftLogWriter struct {
	logger gokitlog.Logger
}

func (o *raftLogWriter) Write(line []byte) (n int, err error) {
	// Parse the log level according to hashicorp's raft pkg convetions.
	var level, msg string
	x := bytes.IndexByte(line, '[')
	if x >= 0 {
		y := bytes.IndexByte(line[x:], ']')
		if y >= 0 {
			level = string(line[x+1 : x+y])

			// Capitalize the string, to match logging conventions
			first := strings.ToUpper(string(line[x+y+2]))
			rest := string(line[x+y+3 : len(line)-1])
			msg = first + rest
		}
	}

	if level == "" {
		// Ignore log entries that don't stick to the convetion.
		return len(line), nil
	}

	switch level {
	case "DEBUG":
		gokitlevel.Debug(o.logger).Log("msg", msg)
	case "INFO":
		gokitlevel.Info(o.logger).Log("msg", msg)
	case "WARN":
		gokitlevel.Warn(o.logger).Log("msg", msg)
	default:
		// Ignore any other log level.
	}
	return len(line), nil
}

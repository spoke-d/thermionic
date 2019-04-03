package main

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-logfmt/logfmt"
	"github.com/spoke-d/clui"
)

type logCluiEncoder struct {
	*logfmt.Encoder
	buf bytes.Buffer
}

func (l *logCluiEncoder) Reset() {
	l.Encoder.Reset()
	l.buf.Reset()
}

var logCluiEncoderPool = sync.Pool{
	New: func() interface{} {
		var enc logCluiEncoder
		enc.Encoder = logfmt.NewEncoder(&enc.buf)
		return &enc
	},
}

type logCluiFormatter struct {
	ui clui.UI
}

// NewLogCluiFormatter returns a logger that encodes keyvals to the Writer in
// logfmt format. Each log event produces no more than one call to w.Write.
// The passed Writer must be safe for concurrent use by multiple goroutines if
// the returned Logger will be used concurrently.
func NewLogCluiFormatter(ui clui.UI) log.Logger {
	return &logCluiFormatter{
		ui: ui,
	}
}

func (l logCluiFormatter) Log(keyvals ...interface{}) error {
	enc := logCluiEncoderPool.Get().(*logCluiEncoder)
	enc.Reset()
	defer logCluiEncoderPool.Put(enc)

	if err := enc.EncodeKeyvals(keyvals...); err != nil {
		return err
	}

	// Add newline to the end of the buffer
	if err := enc.EndRecord(); err != nil {
		return err
	}
	for i := 0; i < len(keyvals)-1; i += 2 {
		if keyvals[i] == "level" {
			output := strings.TrimSuffix(enc.buf.String(), "\n")
			var level string
			if levelValue, ok := keyvals[i+1].(fmt.Stringer); ok {
				level = levelValue.String()
			}
			switch level {
			case "info":
				l.ui.Info(output)
			case "warn":
				l.ui.Warn(output)
			case "error":
				l.ui.Error(output)
			default:
				l.ui.Output(output)
			}
		}
	}
	return nil
}

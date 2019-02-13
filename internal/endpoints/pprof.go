package endpoints

import (
	"net"
	"net/http"

	_ "net/http/pprof" // pprof magic

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

func pprofCreateServer() *http.Server {
	// Undo the magic that importing pprof does
	pprofMux := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()

	// Setup an http server
	return &http.Server{
		Handler: pprofMux,
	}
}

func pprofCreateListener(address string, logger log.Logger) net.Listener {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		level.Error(logger).Log("msg", "Cannot listen on https socket, skipping...", "err", err)
		return nil
	}
	return listener
}

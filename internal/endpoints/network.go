package endpoints

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// Create a new net.Listener bound to the tcp socket of the network endpoint.
func networkCreateListener(address string, cert *cert.Info, logger log.Logger) net.Listener {
	listener, err := net.Listen("tcp", canonicalNetworkAddress(address))
	if err != nil {
		level.Error(logger).Log("msg", "Cannot listen on https socket, skipping...", "err", err)
		return nil
	}
	return networkTLSListener(listener, cert, logger)
}

// canonicalNetworkAddress parses the given network address and returns a
// string of the form "host:port", possibly filling it with the default port if
// it's missing.
func canonicalNetworkAddress(address string) string {
	_, _, err := net.SplitHostPort(address)
	if err != nil {
		ip := net.ParseIP(address)
		if ip != nil && ip.To4() == nil {
			address = fmt.Sprintf("[%s]:%s", address, DefaultPort)
		} else {
			address = fmt.Sprintf("%s:%s", address, DefaultPort)
		}
	}
	return address
}

// A variation of the standard tls.Listener that supports atomically swapping
// the underlying TLS configuration. Requests served before the swap will
// continue using the old configuration.
type networkListener struct {
	net.Listener
	mutex  sync.RWMutex
	config *tls.Config
	logger log.Logger
}

func networkTLSListener(inner net.Listener, cert *cert.Info, logger log.Logger) *networkListener {
	listener := &networkListener{
		Listener: inner,
		logger:   logger,
	}
	listener.Config(cert)
	return listener
}

// Accept waits for and returns the next incoming TLS connection then use the
// current TLS configuration to handle it.
func (l *networkListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	l.mutex.RLock()
	defer l.mutex.RUnlock()

	config := l.config
	return tls.Server(c, config), nil
}

// Config safely swaps the underlying TLS configuration.
func (l *networkListener) Config(certInfo *cert.Info) {
	config := cert.ServerTLSConfig(certInfo, l.logger)

	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.config = config
}

package net

import (
	"net"
	"time"

	"github.com/pkg/errors"
)

func RFC3493Dialer(network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	addrs, err := net.LookupHost(host)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, a := range addrs {
		c, err := net.DialTimeout(network, net.JoinHostPort(a, port), 10*time.Second)
		if err != nil {
			continue
		}
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetKeepAlive(true)
			tc.SetKeepAlivePeriod(3 * time.Second)
		}
		return c, errors.WithStack(err)
	}
	return nil, errors.Errorf("unable to connect to %q", address)
}

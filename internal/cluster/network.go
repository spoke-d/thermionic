package cluster

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/heartbeat"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/eagain"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// Create a dial function that connects to the given listener.
func dqliteMemoryDial(provider Net, listener net.Listener) dqlite.DialFunc {
	return func(ctx context.Context, address string) (net.Conn, error) {
		return provider.UnixDial(listener.Addr().String())
	}
}

func dqliteNetworkDial(ctx context.Context, addr string, certInfo *cert.Info) (net.Conn, error) {
	config, err := cert.TLSClientConfig(certInfo)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Make a probe HEAD request to check if the target node is the leader.
	path := fmt.Sprintf("https://%s%s", addr, heartbeat.DatabaseEndpoint)
	request, err := http.NewRequest("HEAD", path, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	request = request.WithContext(ctx)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: config,
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// If the endpoint does not exists, it means that the target node is
	// running version 1 of dqlite protocol. In that case we simply behave
	// as the node was at an older version.
	if response.StatusCode == http.StatusNotFound {
		return nil, db.ErrSomeNodesAreBehind
	}

	if response.StatusCode != http.StatusOK {
		return nil, errors.Errorf(response.Status)
	}

	// Establish the connection
	request = &http.Request{
		Method:     "POST",
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       addr,
	}
	request.URL, err = url.Parse(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	request.Header.Set("Upgrade", "dqlite")
	request = request.WithContext(ctx)

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now()
	}
	dialer := &net.Dialer{
		Timeout: time.Until(deadline),
	}

	conn, err := tls.DialWithDialer(dialer, "tcp", addr, config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to HTTP endpoint")
	}

	err = request.Write(conn)
	if err != nil {
		return nil, errors.Wrap(err, "sending HTTP request failed")
	}

	response, err = http.ReadResponse(bufio.NewReader(conn), request)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response")
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		return nil, errors.Errorf("dialing failed: expected status code 101 got %d", response.StatusCode)
	}
	if response.Header.Get("Upgrade") != "dqlite" {
		return nil, errors.Errorf("missing or unexpected upgrade header in response")
	}

	return conn, err
}

// DqliteProxyFn is an alias for the type returned by a DqliteProxy
type DqliteProxyFn func(net.Listener, chan net.Conn) error

// DqliteProxy creates a proxy between a provider and a listener. It copies
// information from a destination and a source, before closing both sides.
func DqliteProxy(logger log.Logger, provider Net) DqliteProxyFn {
	return func(listener net.Listener, acceptCh chan net.Conn) error {
		for {
			src := <-acceptCh
			dst, err := provider.UnixDial(listener.Addr().String())
			if err != nil {
				return errors.WithStack(err)
			}

			go func() {
				defer src.Close()

				_, err := io.Copy(eagain.NewWriter(dst), eagain.NewReader(src))
				if err != nil {
					level.Warn(logger).Log("msg", "Error during dqlite proxy copy", "err", err)
				}
			}()

			go func() {
				defer dst.Close()

				_, err := io.Copy(eagain.NewWriter(src), eagain.NewReader(dst))
				if err != nil {
					level.Warn(logger).Log("msg", "Error during dqlite proxy copy", "err", err)
				}
			}()
		}
	}
}

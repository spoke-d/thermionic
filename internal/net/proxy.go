package net

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

// ProxyFromEnvironment is basically the same as golang's ProxyFromEnvironment,
// except it doesn't fall back to http_proxy when https_proxy isn't around,
// which is incorrect behavior. It still respects HTTP_PROXY, HTTPS_PROXY, and
// NO_PROXY.
func ProxyFromEnvironment(req *http.Request) (*url.URL, error) {
	var proxy, port string
	var err error

	switch req.URL.Scheme {
	case "https":
		proxy = httpsProxyEnv.Get()
		port = ":443"
	case "http":
		proxy = httpProxyEnv.Get()
		port = ":80"
	default:
		return nil, errors.Errorf("unknown scheme %q", req.URL.Scheme)
	}

	if proxy == "" {
		return nil, nil
	}

	addr := req.URL.Host
	if !hasPort(addr) {
		addr = addr + port
	}

	use, err := useProxy(addr)
	if err != nil {
		return nil, err
	}
	if !use {
		return nil, nil
	}

	proxyURL, err := url.Parse(proxy)
	if err != nil || !strings.HasPrefix(proxyURL.Scheme, "http") {
		// proxy was bogus. Try prepending "http://" to it and
		// see if that parses correctly. If not, we fall
		// through and complain about the original one.
		if proxyURL, err := url.Parse("http://" + proxy); err == nil {
			return proxyURL, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("invalid proxy address %q: %v", proxy, err)
	}
	return proxyURL, nil
}

var (
	httpProxyEnv = &envOnce{
		names: []string{
			"HTTP_PROXY",
			"http_proxy",
		},
	}
	httpsProxyEnv = &envOnce{
		names: []string{
			"HTTPS_PROXY",
			"https_proxy",
		},
	}
	noProxyEnv = &envOnce{
		names: []string{
			"NO_PROXY",
			"no_proxy",
		},
	}
)

type envOnce struct {
	names []string
	once  sync.Once
	val   string
}

func (e *envOnce) Get() string {
	e.once.Do(e.init)
	return e.val
}

func (e *envOnce) init() {
	for _, n := range e.names {
		if e.val = os.Getenv(n); e.val != "" {
			return
		}
	}
}

func hasPort(s string) bool {
	return strings.LastIndex(s, ":") > strings.LastIndex(s, "]")
}

func useProxy(addr string) (bool, error) {
	noProxy := noProxyEnv.Get()
	if len(addr) == 0 {
		return true, nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, nil
	}
	if host == "localhost" {
		return false, nil
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			return false, nil
		}
	}

	if noProxy == "*" {
		return false, nil
	}

	addr = strings.ToLower(strings.TrimSpace(addr))
	if hasPort(addr) {
		addr = addr[:strings.LastIndex(addr, ":")]
	}

	for _, p := range strings.Split(noProxy, ",") {
		p = strings.ToLower(strings.TrimSpace(p))
		if len(p) == 0 {
			continue
		}
		if hasPort(p) {
			p = p[:strings.LastIndex(p, ":")]
		}
		if addr == p {
			return false, nil
		}
		if p[0] == '.' && (strings.HasSuffix(addr, p) || addr == p[1:]) {
			// noProxy ".foo.com" matches "bar.foo.com" or "foo.com"
			return false, nil
		}
		if p[0] != '.' && strings.HasSuffix(addr, p) && addr[len(addr)-len(p)-1] == '.' {
			// noProxy "foo.com" matches "bar.foo.com"
			return false, nil
		}
	}
	return true, nil
}

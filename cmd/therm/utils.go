package main

import (
	"bytes"
	libjson "encoding/json"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/client"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/sys"
	yaml "gopkg.in/yaml.v2"
)

func contains(a []string, b string) bool {
	for _, v := range a {
		if v == b {
			return true
		}
	}
	return false
}

func nonEmpty(s ...string) bool {
	for _, v := range s {
		if v == "" {
			return false
		}
	}
	return true
}

func empty(s ...string) bool {
	for _, v := range s {
		if v != "" {
			return false
		}
	}
	return true
}

var (
	yes = []string{"yes", "y", "true", "t"}
)

func isTrue(s string) bool {
	return contains(yes, strings.ToLower(s))
}

// PasswordPrompt is used to ask for the certificate password when the cmd is
// run.
type PasswordPrompt func(string) (string, error)

type certs struct {
	serverCert string
	clientCert string
	clientKey  string
}

func getClient(address string, certs certs, prompt PasswordPrompt, logger log.Logger) (*client.Client, error) {
	if !nonEmpty(certs.serverCert, certs.clientCert, certs.clientKey) {
		fileSystem := fsys.NewLocalFileSystem(false)
		os := sys.DefaultOS()
		if err := os.Init(fileSystem); err == nil {
			certs, err = readCertificates(fileSystem, os.VarDir(), prompt)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.Errorf("expected all certificate and keys to provided together")
		}
	}

	return client.New(
		address,
		certs.serverCert,
		certs.clientCert,
		certs.clientKey,
		client.WithLogger(log.WithPrefix(logger, "component", "client")),
	)
}

func outputContent(format string, value interface{}) ([]byte, error) {
	switch format {
	case "yaml":
		return yaml.Marshal(value)
	case "json":
		return libjson.MarshalIndent(value, "", "\t")
	case "tabular":
		buf := new(bytes.Buffer)
		w := tabwriter.NewWriter(buf, 2, 2, 3, ' ', 0)

		var headers []string
		var values []string

		t := reflect.TypeOf(value)
		v := reflect.ValueOf(value)
		switch t.Kind() {
		case reflect.Map:
			for _, idx := range v.MapKeys() {
				out, err := outputContent("tabular", struct {
					Key   interface{}
					Value interface{}
				}{
					Key:   idx.Interface(),
					Value: v.MapIndex(idx).Interface(),
				})
				if err != nil {
					return nil, errors.WithStack(err)
				}
				fmt.Fprintln(buf, string(out))
			}
		case reflect.Struct:
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				tab := field.Tag.Get("tab")
				if tab == "" {
					tab = field.Name
				}
				headers = append(headers, tab)
				values = append(values, fmt.Sprintf("%v", v.Field(i).Interface()))
			}
			fmt.Fprintln(w, strings.Join(headers, "\t"))
			fmt.Fprintln(w, strings.Join(values, "\t"))
		default:
			fmt.Fprintln(w, "Value\t")
			fmt.Fprintln(w, fmt.Sprintf("%v\t", v.String()))
		}

		if err := w.Flush(); err != nil {
			return nil, errors.WithStack(err)
		}
		return buf.Bytes(), nil
	}
	return nil, errors.Errorf("unexpected format %q", format)
}

func outputFieldContent(format string, config map[string]interface{}, key string) ([]byte, error) {
	if x, ok := config[key]; ok {
		switch format {
		case "yaml":
			return yaml.Marshal(x)
		case "json":
			return libjson.Marshal(x)
		}
		return nil, errors.Errorf("unexpected format %q", format)
	}
	return nil, errors.Errorf("key not found %q", key)
}

// "udp://host:1234", 80 => udp host:1234 host 1234
// "host:1234", 80       => tcp host:1234 host 1234
// "host", 80            => tcp host:80   host 80
func parseAddr(addr string, defaultPort int) (network, address string, err error) {
	u, err := url.Parse(strings.ToLower(addr))
	if err != nil {
		return "", "", errors.WithStack(err)
	}

	switch {
	case u.Scheme == "" && u.Opaque == "" && u.Host == "" && u.Path != "": // "host"
		u.Scheme, u.Opaque, u.Host, u.Path = "tcp", "", net.JoinHostPort(u.Path, strconv.Itoa(defaultPort)), ""
	case u.Scheme != "" && u.Opaque != "" && u.Host == "" && u.Path == "": // "host:1234"
		u.Scheme, u.Opaque, u.Host, u.Path = "tcp", "", net.JoinHostPort(u.Scheme, u.Opaque), ""
	case u.Scheme != "" && u.Opaque == "" && u.Host != "" && u.Path == "": // "tcp://host[:1234]"
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			u.Host = net.JoinHostPort(u.Host, strconv.Itoa(defaultPort))
		}
	default:
		return network, address, errors.Errorf("%s: unsupported address format", addr)
	}

	return u.Scheme, u.Host, nil
}

// parseDiscoveryAddr attempts to parse a url into a host and port.
func parseDiscoveryAddr(addr string, defaultPort int) (string, int, error) {
	if !strings.Contains(addr, "://") {
		addr = "tcp://" + addr
	}
	_, hostAddr, err := parseAddr(addr, defaultPort)
	if err != nil {
		return "", -1, errors.WithStack(err)
	}
	host, portStr, err := net.SplitHostPort(hostAddr)
	if err != nil {
		return "", -1, errors.WithStack(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", -1, errors.WithStack(err)
	}
	return host, port, nil
}

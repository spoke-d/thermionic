package endpoints_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"testing"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/endpoints"
	"github.com/spoke-d/thermionic/internal/json"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
	golog "github.com/go-kit/kit/log"
)

func newEndpoints(t *testing.T, options ...endpoints.Option) (*endpoints.Endpoints, func()) {
	e := endpoints.New(newServer(), libtesting.KeyPair(), options...)

	return e, func() {
		if err := e.Down(); err != nil {
			t.Error(err)
		}
	}
}

// Returns a minimal stub for the RESTful API server
func newServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/1.0/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.Write(w, struct{}{}, false, golog.NewNopLogger())
	})
	return &http.Server{
		Handler:  mux,
		ErrorLog: log.New(ioutil.Discard, "", 0),
	}
}

// Perform an HTTP GET "/" over TLS, using the given network address and server
// certificate.
func httpGet(addr string) error {
	client := http.DefaultClient
	_, err := client.Get(fmt.Sprintf("http://%s", addr))
	return err
}

// Perform an HTTP GET "/" over TLS, using the given network address and server
// certificate.
func httpGetOverTLSSocket(addr string, certInfo *cert.Info) error {
	tlsConfig, _ := cert.GetTLSConfigMem("", "", "", string(certInfo.PublicKey()), false)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
	_, err := client.Get(fmt.Sprintf("https://%s/", addr))
	return err
}

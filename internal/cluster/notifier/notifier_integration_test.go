// +build integration

package notifier_test

import (
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/notifier"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/state"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
	"github.com/spoke-d/thermionic/pkg/client"
)

// The returned notifier connects to all nodes.
func TestNewNotifier(t *testing.T) {
	state, cleanup := libtesting.NewTestState(t, 1, 1)
	defer cleanup()

	cert := libtesting.KeyPair()

	f := fixtures{
		t:     t,
		state: state,
	}
	defer f.Nodes(cert, 3)()

	notifierTask := notifier.New(notifyStateShim{
		state: state,
	}, cert, node.ConfigSchema)
	peers := make(chan string, 2)
	hook := func(client *client.Client) error {
		resp, _, err := client.Query("GET", "/1.0", nil, "")
		if err != nil {
			t.Error(err)
		}
		var metadata map[string]map[string]string
		if err := stdjson.Unmarshal(resp.Metadata, &metadata); err != nil {
			t.Error(err)
		}
		peers <- metadata["config"]["core.https_address"]
		return nil
	}
	if err := notifierTask.Run(hook, notifier.NotifyAll); err != nil {
		t.Error(err)
	}

	addresses := make([]string, 2)
	for i := range addresses {
		select {
		case addresses[i] = <-peers:
		default:
		}
	}
	for i := range addresses {
		if expected, actual := true, contains(addresses, f.Address(i+1)); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	}
}

// Creating a new notifier fails if the policy is set to NotifyAll and one of
// the nodes is down.
func TestNewNotify_NotifyAllError(t *testing.T) {
	state, cleanup := libtesting.NewTestState(t, 1, 1)
	defer cleanup()

	cert := libtesting.KeyPair()

	f := fixtures{
		t:     t,
		state: state,
	}
	defer f.Nodes(cert, 3)()

	hook := func(client *client.Client) error {
		return nil
	}

	f.Down(1)
	notifierTask := notifier.New(notifyStateShim{
		state: state,
	}, cert, node.ConfigSchema)
	err := notifierTask.Run(hook, notifier.NotifyAll)
	if err == nil {
		t.Error(err)
	}
	if expected, actual := "peer node \".+\" is down", errors.Cause(err).Error(); !testString(t, expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Creating a new notifier does not fail if the policy is set to NotifyAlive
// and one of the nodes is down, however dead nodes are ignored.
func TestNewNotify_NotifyAlive(t *testing.T) {
	state, cleanup := libtesting.NewTestState(t, 1, 1)
	defer cleanup()

	cert := libtesting.KeyPair()

	f := fixtures{
		t:     t,
		state: state,
	}
	defer f.Nodes(cert, 3)()

	i := 0
	hook := func(client *client.Client) error {
		i++
		return nil
	}

	f.Down(1)
	notifierTask := notifier.New(notifyStateShim{
		state: state,
	}, cert, node.ConfigSchema)
	err := notifierTask.Run(hook, notifier.NotifyAlive)
	if err != nil {
		t.Error(err)
	}
	if expected, actual := 1, i; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func testString(t *testing.T, expected, actual string) bool {
	t.Helper()

	ok, err := regexp.MatchString(expected, actual)
	if err != nil {
		t.Error(err)
		return false
	}
	return ok
}

// Helper for setting fixtures for Notify tests.
type fixtures struct {
	t     *testing.T
	state *state.State
}

// Spawn the given number of fake nodes, save in them in the database and
// return a cleanup function.
//
// The address of the first node spawned will be saved as local
// core.https_address.
func (h *fixtures) Nodes(cert *cert.Info, n int) func() {
	servers := make([]*httptest.Server, n)
	for i := 0; i < n; i++ {
		servers[i] = newRestServer(cert)
	}

	// Insert new entries in the nodes table of the cluster database.
	if err := h.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		for i := 0; i < n; i++ {
			name := strconv.Itoa(i)
			address := servers[i].Listener.Addr().String()
			var err error
			if i == 0 {
				err = tx.NodeUpdate(int64(1), name, address)
			} else {
				_, err = tx.NodeAdd(name, address, 1, 1)
			}
			if err != nil {
				h.t.Error(err)
			}
		}
		return nil
	}); err != nil {
		h.t.Error(err)
	}

	// Set the address in the config table of the node database.
	if err := h.state.Node().Transaction(func(tx *db.NodeTx) error {
		config, err := node.ConfigLoad(tx, node.ConfigSchema)
		if err != nil {
			h.t.Error(err)
		}
		address := servers[0].Listener.Addr().String()
		values := map[string]interface{}{
			"core.https_address": address,
		}
		_, err = config.Patch(values)
		if err != nil {
			h.t.Error(err)
		}
		return nil
	}); err != nil {
		h.t.Error(err)
	}

	return func() {
		for _, server := range servers {
			server.Close()
		}
	}
}

// Return the network address of the i-th node.
func (h *fixtures) Address(i int) string {
	var address string
	if err := h.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		nodes, err := tx.Nodes()
		if err != nil {
			h.t.Error(err)
		}
		address = nodes[i].Address
		return nil
	}); err != nil {
		h.t.Error(err)
	}
	return address
}

// Mark the i'th node as down.
func (h *fixtures) Down(i int) {
	if err := h.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		nodes, err := tx.Nodes()
		if err != nil {
			h.t.Error(err)
		}
		err = tx.NodeHeartbeat(nodes[i].Address, time.Now().Add(-time.Minute))
		if err != nil {
			h.t.Error(err)
		}
		return nil
	}); err != nil {
		h.t.Error(err)
	}
}

// Returns a minimal stub for the RESTful API server, just realistic
// enough to make connect succeed.
func newRestServer(certInfo *cert.Info) *httptest.Server {
	mux := http.NewServeMux()

	server := httptest.NewUnstartedServer(mux)
	server.TLS = cert.ServerTLSConfig(certInfo, log.NewNopLogger())
	server.StartTLS()

	mux.HandleFunc("/1.0/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		config := map[string]interface{}{
			"core.https_address": server.Listener.Addr().String(),
		}
		json.Write(w, client.ResponseRaw{
			Metadata: struct {
				Config map[string]interface{} `json:"config"`
			}{
				Config: config,
			},
		}, false, log.NewNopLogger())
	})

	return server
}

func contains(a []string, b string) bool {
	for _, v := range a {
		if v == b {
			return true
		}
	}
	return false
}

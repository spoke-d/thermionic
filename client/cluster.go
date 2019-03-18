package client

import (
	"bytes"
	libjson "encoding/json"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/pkg/api/daemon/cluster"
	"github.com/spoke-d/thermionic/pkg/api/daemon/operations"
	"github.com/spoke-d/thermionic/pkg/client"
	"github.com/spoke-d/thermionic/pkg/events"
)

// ClusterCertInfo is used for joining a cluster
type ClusterCertInfo struct {
	Certificate string
	Key         string
}

// Cluster represents a way of interacting with the daemon API, which is
// responsible for getting the cluster information from the daemon.
type Cluster struct {
	client *Client
}

// Join will attempt to join the cluster using the node name
func (c *Cluster) Join(name, address, etag string, certInfo ClusterCertInfo) error {
	info := cluster.ClusterJoin{
		Cluster: cluster.Cluster{
			ServerName: name,
			Enabled:    true,
		},
		ClusterAddress:     address,
		ClusterCertificate: certInfo.Certificate,
		ClusterKey:         certInfo.Key,
	}

	eventsTask := events.NewEvents(c.client.RawClient())
	listener, err := eventsTask.GetEvents()
	if err != nil {
		listener = nil
	}

	var op operations.Op
	if err := c.client.exec("PUT", "/1.0/cluster", info, etag, func(response *client.Response, meta Metadata) error {
		if err := json.Read(bytes.NewReader(response.Metadata), &op); err != nil {
			if listener != nil {
				listener.Disconnect()
			}
			return errors.Wrap(err, "error parsing operation")
		}
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}

	localOp := &operation{
		op:         op,
		client:     c.client,
		listener:   listener,
		activeChan: make(chan bool),
	}

	return localOp.Wait()
}

// Remove will attempt to remove the cluster using the node name
func (c *Cluster) Remove(name string, force bool) error {
	var forceQuery string
	if force {
		forceQuery = "?force=1"
	}

	err := c.client.exec("DELETE", fmt.Sprintf("/1.0/cluster/members/%s%s", name, forceQuery), nil, "", nopReadFn)
	return errors.WithStack(err)
}

// Enabled returns if the cluster is enabled or not
func (c *Cluster) Enabled() (bool, string, error) {
	var enabled bool
	var etag string
	if err := c.client.exec("GET", "/1.0/cluster", nil, "", func(response *client.Response, meta Metadata) error {
		var clusterInfo cluster.Cluster
		if err := json.Read(bytes.NewReader(response.Metadata), &clusterInfo); err != nil {
			return errors.Wrap(err, "error parsing result")
		}
		enabled = clusterInfo.Enabled
		etag = meta.ETag
		return nil
	}); err != nil {
		return false, "", errors.WithStack(err)
	}
	return enabled, etag, nil
}

// Info returns the cluster information
func (c *Cluster) Info() (ClusterInfo, error) {
	var info ClusterInfo
	if err := c.client.exec("GET", "/1.0/cluster", nil, "", func(response *client.Response, meta Metadata) error {
		var clusterInfo cluster.Cluster
		if err := json.Read(bytes.NewReader(response.Metadata), &clusterInfo); err != nil {
			return errors.Wrap(err, "error parsing result")
		}
		info = ClusterInfo{
			ServerName: clusterInfo.ServerName,
			Enabled:    clusterInfo.Enabled,
		}
		return nil
	}); err != nil {
		return ClusterInfo{}, errors.WithStack(err)
	}
	return info, nil
}

// ClusterInfo represents high-level information about the cluster.
type ClusterInfo struct {
	ServerName string `json:"server_name" yaml:"server_name"`
	Enabled    bool   `json:"enabled" yaml:"enabled"`
}

type operation struct {
	op           operations.Op
	client       *Client
	listener     *events.EventListener
	activeChan   chan bool
	mutex        sync.Mutex
	handlerReady bool
}

// Wait lets you wait until the operation reaches a final state
func (o *operation) Wait() error {
	if operations.Status(o.op.StatusCode).IsFinal() {
		if o.op.Err != "" {
			return errors.Wrap(errors.New(o.op.Err), "failed to request status code")
		}
	}

	// Make sure we have a listener setup
	if err := o.setupListener(); err != nil {
		return errors.Wrap(err, "failed setting up listener")
	}

	<-o.activeChan

	if o.op.Err != "" {
		return errors.Wrap(errors.New(o.op.Err), "failed to wait")
	}
	return nil
}

func (o *operation) setupListener() error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.handlerReady {
		return nil
	}
	o.handlerReady = true

	var err error
	eventsTask := events.NewEvents(o.client.RawClient())
	o.listener, err = eventsTask.GetEvents()
	if err != nil {
		return errors.Wrap(err, "failed to request events")
	}

	// setup handler
	readyChan := make(chan bool)
	o.listener.AddHandler([]string{
		"operation",
	}, func(data interface{}) {
		<-readyChan

		// We don't want concurrency while processing events
		o.mutex.Lock()
		defer o.mutex.Unlock()

		if o.listener == nil {
			return
		}

		newOp := extractOperation(data, o.op.ID)
		if newOp == nil {
			return
		}

		o.op = *newOp

		if operations.Status(o.op.StatusCode).IsFinal() {
			o.listener.Disconnect()
			o.listener = nil
			close(o.activeChan)
			return
		}
	})

	// Monitor event listener
	go func() {
		<-readyChan

		// We don't want concurrency while accessing the listener
		o.mutex.Lock()
		listener := o.listener
		if listener == nil {
			o.mutex.Unlock()
			return
		}
		o.mutex.Unlock()

		select {
		case <-listener.ActiveChan():
			o.mutex.Lock()
			if o.listener != nil {
				o.op.Err = fmt.Sprintf("%v", listener.Err())
				close(o.activeChan)
			}
			o.mutex.Unlock()
		case <-o.activeChan:
			return
		}
	}()

	if err := o.refresh(); err != nil {
		o.listener.Disconnect()
		o.listener = nil
		close(o.activeChan)
		close(readyChan)

		return errors.Wrap(err, "failed refreshing operation")
	}

	if operations.Status(o.op.StatusCode).IsFinal() {
		o.listener.Disconnect()
		o.listener = nil
		close(o.activeChan)
		close(readyChan)

		if o.op.Err != "" {
			return errors.Wrap(errors.New(o.op.Err), "failed to request status code")
		}
		return nil
	}

	close(readyChan)
	return nil
}

func (o *operation) refresh() error {
	response, _, err := o.client.Query("GET", fmt.Sprintf("/1.0/operations/%s", o.op.ID), nil, "")
	if err != nil {
		return errors.Wrap(err, "failed to request operation")
	} else if response.StatusCode != 200 {
		return errors.Errorf("invalid status code %d", response.StatusCode)
	}

	var op operations.Op
	if err := json.Read(bytes.NewReader(response.Metadata), &op); err != nil {
		return errors.Wrap(err, "error parsing operation")
	}

	o.op = op

	return nil
}

func extractOperation(data interface{}, id string) *operations.Op {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}
	meta, ok := m["metadata"]
	if !ok {
		return nil
	}

	encoded, err := libjson.Marshal(meta)
	if err != nil {
		return nil
	}

	var op operations.Op
	if err := libjson.Unmarshal(encoded, &op); err != nil {
		return nil
	}

	if op.ID != id {
		return nil
	}
	return &op
}

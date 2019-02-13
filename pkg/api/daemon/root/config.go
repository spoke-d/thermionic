package root

import (
	"bytes"

	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/cluster/notifier"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/json"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/pkg/api"
	"github.com/spoke-d/thermionic/pkg/client"
	"github.com/pkg/errors"
)

func readConfig(
	c api.Cluster,
	n api.Node,
	clusterConfigSchema,
	nodeConfigSchema config.Schema,
) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	if err := c.Transaction(func(tx *db.ClusterTx) error {
		clusterConfig, err := clusterconfig.Load(tx, clusterConfigSchema)
		if err != nil {
			return errors.WithStack(err)
		}
		raw, err := clusterConfig.Dump()
		if err != nil {
			return errors.WithStack(err)
		}
		for key, value := range raw {
			result[key] = value
		}
		return nil
	}); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := n.Transaction(func(tx *db.NodeTx) error {
		nodeConfig, err := node.ConfigLoad(tx, nodeConfigSchema)
		if err != nil {
			return errors.WithStack(err)
		}
		raw, err := nodeConfig.Dump()
		if err != nil {
			return errors.WithStack(err)
		}
		for key, value := range raw {
			result[key] = value
		}
		return nil
	}); err != nil {
		return nil, errors.WithStack(err)
	}

	return result, nil
}

func update(d api.Daemon, req ServerUpdate, patch bool) api.Response {
	nodeValues := make(map[string]interface{})
	for key := range node.ConfigSchema {
		if value, ok := req.Config[key]; ok {
			nodeValues[key] = value
			delete(req.Config, key)
		}
	}

	var newNodeConfig *node.Config
	var nodeChanged map[string]string
	if err := d.Node().Transaction(func(tx *db.NodeTx) error {
		var err error
		newNodeConfig, err = node.ConfigLoad(tx, d.NodeConfigSchema())
		if err != nil {
			return errors.Wrap(err, "failed to load node config")
		}
		if patch {
			nodeChanged, err = newNodeConfig.Patch(nodeValues)
		} else {
			nodeChanged, err = newNodeConfig.Replace(nodeValues)
		}
		return errors.Wrap(err, "attempting to write node config")
	}); err != nil {
		switch errors.Cause(err).(type) {
		case config.ErrorList:
			return api.BadRequest(err)
		default:
			return api.SmartError(err)
		}
	}

	// deal with cluster wide configurations
	var newClusterConfig *clusterconfig.Config
	var clusterChanged map[string]string
	if err := d.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		newClusterConfig, err = clusterconfig.Load(tx, d.ClusterConfigSchema())
		if err != nil {
			return errors.Wrap(err, "failed to load node config")
		}
		if patch {
			clusterChanged, err = newClusterConfig.Patch(req.Config)
		} else {
			clusterChanged, err = newClusterConfig.Replace(req.Config)
		}
		return errors.Wrap(err, "attempting to write cluster config")
	}); err != nil {
		switch errors.Cause(err).(type) {
		case config.ErrorList:
			return api.BadRequest(err)
		default:
			return api.SmartError(err)
		}
	}

	// Notify the other nodes about changes
	notifierTask := notifier.New(
		makeNotifierStateShim(d.State()),
		d.Endpoints().NetworkCert(),
		d.NodeConfigSchema(),
	)
	if err := notifierTask.Run(updateClusterConfig(clusterChanged), notifier.NotifyAlive); err != nil {
		return api.SmartError(err)
	}

	if err := updateClusterTriggers(d, clusterChanged, newClusterConfig); err != nil {
		return api.SmartError(err)
	}
	if err := updateNodeTriggers(d, nodeChanged, newNodeConfig); err != nil {
		return api.SmartError(err)
	}
	return api.EmptySyncResponse()
}

func updateClusterConfig(changes map[string]string) func(*client.Client) error {
	return func(client *client.Client) error {
		resp, etag, err := client.Query("GET", "/1.0", nil, "")
		if err != nil {
			return errors.WithStack(err)
		} else if resp.StatusCode != 200 {
			return errors.Errorf("invalid status code for requesting, received %d", resp.StatusCode)
		}

		var res ServerUpdate
		if err := json.Read(bytes.NewReader(resp.Metadata), &res); err != nil {
			return errors.WithStack(err)
		}

		for key, value := range changes {
			res.Config[key] = value
		}

		resp, _, err = client.Query("PUT", "/1.0", res, etag)
		if err != nil {
			return errors.WithStack(err)
		} else if resp.StatusCode != 200 {
			return errors.Errorf("invalid status code for update, received %d", resp.StatusCode)
		}

		return nil
	}
}

func updateClusterTriggers(d api.Daemon, changes map[string]string, config *clusterconfig.Config) error {
	// TODO: ensure that we apply any updates to the daemon
	return nil
}

func updateNodeTriggers(d api.Daemon, changes map[string]string, config *node.Config) error {
	for key, value := range changes {
		switch key {
		case "core.https_address":
			if err := d.Endpoints().NetworkUpdateAddress(value); err != nil {
				return errors.WithStack(err)
			}
		case "core.debug_address":
			if err := d.Endpoints().PprofUpdateAddress(value); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

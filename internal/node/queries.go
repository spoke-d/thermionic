package node

import (
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
)

// Node mediates access to the data stored in the node-local SQLite database.
type Node interface {
	db.NodeTransactioner
}

// HTTPSAddress is a convenience for loading the node configuration and
// returning the value of core.https_address.
func HTTPSAddress(node Node, schema config.Schema) (string, error) {
	var config *Config
	err := node.Transaction(func(tx *db.NodeTx) error {
		var err error
		config, err = ConfigLoad(tx, schema)
		return errors.WithStack(err)
	})
	if err != nil {
		return "", errors.WithStack(err)
	}

	addr, err := config.HTTPSAddress()
	if err != nil {
		return "", errors.WithStack(err)
	}
	return addr, nil
}

// DebugAddress is a convenience for loading the node configuration and
// returning the value of core.debug_address.
func DebugAddress(node Node, schema config.Schema) (string, error) {
	var config *Config
	err := node.Transaction(func(tx *db.NodeTx) error {
		var err error
		config, err = ConfigLoad(tx, schema)
		return errors.WithStack(err)
	})
	if err != nil {
		return "", errors.WithStack(err)
	}

	return config.DebugAddress()
}

// PatchConfig is a convenience for patching the node configuration
func PatchConfig(node Node, key string, value interface{}, schema config.Schema) error {
	if err := node.Transaction(func(tx *db.NodeTx) error {
		config, err := ConfigLoad(tx, schema)
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = config.Patch(map[string]interface{}{
			key: value,
		})
		return errors.WithStack(err)
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

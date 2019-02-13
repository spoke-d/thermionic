package node

import "github.com/spoke-d/thermionic/internal/fsys"

// Rexport the hook and check functions for testing.
var (
	Hook = hook
)

type Context = context

func NewNodeWithMocks(databaseIO DatabaseIO,
	schemaProvider SchemaProvider,
	fileSystem fsys.FileSystem,
) *Node {
	return &Node{
		databaseIO:     databaseIO,
		schemaProvider: schemaProvider,
		fileSystem:     fileSystem,
	}
}

func NewSchemaProviderWithMocks(fileSystem fsys.FileSystem) SchemaProvider {
	return &schemaProvider{
		fileSystem: fileSystem,
	}
}

package schema

// Rexport the local functions for testing.
var (
	ExecFromFile                   = execFromFile
	QueryCurrentVersion            = queryCurrentVersion
	EnsureSchemaTableExists        = ensureSchemaTableExists
	EnsureUpdatesAreApplied        = ensureUpdatesAreApplied
	CheckSchemaVersionsHaveNoHoles = checkSchemaVersionsHaveNoHoles
)

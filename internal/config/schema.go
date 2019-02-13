package config

import (
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Schema defines the available keys of a config Map, along with the types
// and options for their values, expressed using Key objects.
type Schema map[string]Key

// Keys returns all keys defined in the schema
func (s Schema) Keys() []string {
	var i int
	keys := make([]string, len(s))
	for key := range s {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	return keys
}

// Defaults returns a map of all key names in the schema along with their default
// values.
func (s Schema) Defaults() map[string]interface{} {
	values := make(map[string]interface{}, len(s))
	for name, key := range s {
		values[name] = key.Default
	}
	return values
}

// getKey retrives the Key associated with the given name.
func (s Schema) getKey(name string) (Key, error) {
	key, ok := s[name]
	if !ok {
		return Key{}, errors.Errorf("attempt to access unknown key %q", name)
	}
	return key, nil
}

// Assert that the Key with the given name as the given type. Return error if no
// Key with such name exists, or if it does not match the given type.
func (s Schema) assertKeyType(name string, code Type) error {
	key, err := s.getKey(name)
	if err != nil {
		return err
	}
	if key.Type != code {
		return errors.Errorf("key '%s' has type code %d, not %d", name, key.Type, code)
	}
	return nil
}

// Key defines the type of the value of a particular config key, along with
// other knobs such as default, validator, etc.
type Key struct {
	Type       Type   // Type of the value. It defaults to String.
	Default    string // If the key is not set in a Map, use this value instead.
	Hidden     bool   // Hide this key when dumping the object.
	Deprecated string // Optional message to set if this config value is deprecated.

	// Optional function used to validate the values. It's called by Map
	// all the times the value associated with this Key is going to be
	// changed.
	Validator func(string) error

	// Optional function to manipulate a value before it's actually saved
	// in a Map. It's called only by Map.Change(), and not by Load() since
	// values passed to Load() are supposed to have been previously
	// processed.
	Setter func(string) (string, error)
}

// Tells if the given value can be assigned to this particular Value instance.
func (v *Key) validate(value string) error {
	validator := v.Validator
	if validator == nil {
		// Dummy validator
		validator = func(string) error { return nil }
	}

	// Handle unsetting
	if value == "" {
		return validator(v.Default)
	}

	switch v.Type {
	case String:
	case Bool:
		if !contains(strings.ToLower(value), booleans) {
			return errors.Errorf("invalid boolean")
		}
	case Int64:
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return errors.Errorf("invalid integer")
		}
	default:
		return errors.Errorf("unexpected value type: %d", v.Type)
	}

	if v.Deprecated != "" && value != v.Default {
		return errors.Errorf("deprecated: %s", v.Deprecated)
	}

	// Run external validation function
	return validator(value)
}

// Type is a numeric code indetifying a node value type.
type Type int

// Possible Value types.
const (
	String Type = iota
	Bool
	Int64
)

var booleans = []string{
	"true", "false",
	"1", "0",
	"yes", "no",
	"on", "off",
}
var truthy = []string{
	"true",
	"1",
	"yes",
	"on",
}

func contains(key string, list []string) bool {
	for _, entry := range list {
		if entry == key {
			return true
		}
	}
	return false
}

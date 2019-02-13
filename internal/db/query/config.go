package query

import (
	"fmt"
	"strings"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/pkg/errors"
)

// SelectConfig executes a query statement against a "config" table, which must
// have 'key' and 'value' columns. By default this query returns all keys, but
// additional WHERE filters can be specified.
//
// Returns a map of key names to their associated values.
func SelectConfig(tx database.Tx, table string, where string, args ...interface{}) (map[string]string, error) {
	query := fmt.Sprintf("SELECT key, value FROM %s", table)
	if where != "" {
		query += fmt.Sprintf(" WHERE %s", where)
	}

	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()

	values := make(map[string]string)
	for rows.Next() {
		var key string
		var value string

		if err := rows.Scan(&key, &value); err != nil {
			return nil, errors.WithStack(err)
		}
		values[key] = value
	}

	err = rows.Err()
	return values, errors.WithStack(err)
}

// UpdateConfig updates the given keys in the given table. Config keys set to
// empty values will be deleted.
func UpdateConfig(tx database.Tx, table string, values map[string]string) error {
	var deletes []string
	changes := make(map[string]string)

	for key, value := range values {
		if value == "" {
			deletes = append(deletes, key)
			continue
		}
		changes[key] = value
	}

	if err := UpsertConfig(tx, table, changes); err != nil {
		return errors.Wrap(err, "updating values failed")
	}
	if err := DeleteConfig(tx, table, deletes); err != nil {
		return errors.Wrap(err, "deleting values failed")
	}

	return nil
}

// UpsertConfig defines a way to Insert or updates the key/value rows of the
// given config table.
func UpsertConfig(tx database.Tx, table string, values map[string]string) error {
	if len(values) == 0 {
		return nil
	}

	var exprs []string
	var params []interface{}

	query := fmt.Sprintf("INSERT OR REPLACE INTO %s (key, value) VALUES", table)
	for key, value := range values {
		exprs = append(exprs, "(?, ?)")
		params = append(params, key)
		params = append(params, value)
	}
	_, err := tx.Exec(fmt.Sprintf("%s %s", query, strings.Join(exprs, ",")), params...)
	return err
}

// DeleteConfig defines a way to delete the given key rows from the given
// config table.
func DeleteConfig(tx database.Tx, table string, keys []string) error {
	n := len(keys)
	if n == 0 {
		return nil
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE key IN %s", table, Params(n))
	values := make([]interface{}, n)
	for i, key := range keys {
		values[i] = key
	}
	_, err := tx.Exec(query, values...)
	return errors.WithStack(err)
}

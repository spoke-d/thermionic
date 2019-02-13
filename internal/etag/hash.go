package etag

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pkg/errors"
)

// Hash hashes the provided data and returns the sha256
func Hash(data interface{}) (string, error) {
	etag := sha256.New()
	if err := json.NewEncoder(etag).Encode(data); err != nil {
		return "", errors.WithStack(err)
	}
	return fmt.Sprintf("%x", etag.Sum(nil)), nil
}

// Check validates the requesting etag against the data provided
func Check(r *http.Request, data interface{}) error {
	match := r.Header.Get("If-Match")
	if match == "" {
		return nil
	}

	hash, err := Hash(data)
	if err != nil {
		return errors.WithStack(err)
	}

	if hash != match {
		return errors.Errorf("etag doesn't match: %q vs %q", hash, match)
	}
	return nil
}

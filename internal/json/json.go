package json

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

func Read(r io.Reader, req interface{}) error {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.WithStack(err)
	}
	err = json.Unmarshal(buf, req)
	return errors.WithStack(err)
}

// Write encodes the body as JSON and sends it back to the client
func Write(w http.ResponseWriter, body interface{}, debug bool, logger log.Logger) error {
	var output io.Writer
	var captured *bytes.Buffer

	output = w
	if debug {
		captured = new(bytes.Buffer)
		output = io.MultiWriter(w, captured)
	}

	err := json.NewEncoder(output).Encode(body)
	if debug {
		Debug(captured, logger)
	}
	return errors.WithStack(err)
}

func Debug(r *bytes.Buffer, logger log.Logger) {
	pretty := new(bytes.Buffer)
	if err := json.Indent(pretty, r.Bytes(), "\t", "\t"); err != nil {
		level.Debug(logger).Log("msg", "error indenting json", "err", err)
		return
	}

	// Print the JSON without the last "\n"
	str := pretty.String()
	level.Debug(logger).Log("msg", str[0:len(str)-1])
}

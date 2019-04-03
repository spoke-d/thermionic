package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/olekukonko/tablewriter"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/spoke-d/clui"
	"github.com/spoke-d/clui/flagset"
	"github.com/spoke-d/thermionic/internal/exec"
	"github.com/spoke-d/thermionic/pkg/api/daemon/query"
	"github.com/spoke-d/thermionic/pkg/client"
	yaml "gopkg.in/yaml.v2"
)

// Client represents a way to interact with the server API
type Client interface {

	// Query allows directly querying the API
	Query(string, string, interface{}, string) (*client.Response, string, error)
}

type queryCmd struct {
	ui      clui.UI
	flagset *flagset.FlagSet

	debug      bool
	address    string
	format     string
	serverCert string
	clientCert string
	clientKey  string
}

// NewQueryCmd creates a Command with sane defaults
func NewQueryCmd(ui clui.UI) clui.Command {
	c := &queryCmd{
		ui:      ui,
		flagset: flagset.NewFlagSet("query", flag.ExitOnError),
	}
	c.init()
	return c
}

func (c *queryCmd) init() {
	c.flagset.BoolVar(&c.debug, "debug", false, "debug logging")
	c.flagset.StringVar(&c.address, "address", "127.0.0.1:8080", "address of the api server")
	c.flagset.StringVar(&c.format, "format", "yaml", "format to output the information json|yaml|tabular")
	c.flagset.StringVar(&c.serverCert, "server-cert", "", "server certificate")
	c.flagset.StringVar(&c.clientCert, "client-cert", "", "client certificate")
	c.flagset.StringVar(&c.clientKey, "client-key", "", "client key")
}

// UI returns a UI for interaction.
func (c *queryCmd) UI() clui.UI {
	return c.ui
}

// FlagSet returns the FlagSet associated with the command. All the flags are
// parsed before running the command.
func (c *queryCmd) FlagSet() *flagset.FlagSet {
	return c.flagset
}

// Help should return a long-form help text that includes the command-line
// usage. A brief few sentences explaining the function of the command, and
// the complete list of flags the command accepts.
func (c *queryCmd) Help() string {
	return `
Usage: 

  query [flags] <local|global> <query>

Description:

  Execute a SQL query against the local or global database
  The local database is specific to the cluster member you
  target the command to, and contains member-specific data
  (such as the member network address).
  The global database is common to all members in the
  cluster, and contains cluster-specific data (such as
  profiles, containers, etc).
  
  If you are running a non-clustered instance, the same
  applies, as that instance is effectively a single-member
  cluster.
  If <query> is the special value "-", then the query is
  read from standard input.
  
  This internal command is mostly useful for debugging and
  disaster recovery.
  This command targets the global database and works in 
  both local and cluster mode.
  
  To access the API for querying, it is expected that you
  provide certificates and keys for the API to verify and 
  authenticate against.

Example:

  therm query local "SELECT key, value FROM config"
  therm query global "SELECT * FROM raft_nodes" 
`
}

// Synopsis should return a one-line, short synopsis of the command.
// This should be short (50 characters of less ideally).
func (c *queryCmd) Synopsis() string {
	return "Query database using sql."
}

// Run should run the actual command with the given CLI instance and
// command-line arguments. It should return the exit status when it is
// finished.
//
// There are a handful of special exit codes that can return documented
// behavioral changes.
func (c *queryCmd) Run() clui.ExitCode {
	// Logging.
	var logger log.Logger
	{
		logLevel := level.AllowInfo()
		if c.debug {
			logLevel = level.AllowAll()
		}
		logger = NewLogCluiFormatter(c.UI())
		logger = log.With(logger,
			"ts", log.DefaultTimestampUTC,
			"uid", uuid.NewRandom().String(),
		)
		logger = level.NewFilter(logger, logLevel)
	}

	outputFormat := c.format
	if !contains([]string{"json", "yaml", "tabular"}, outputFormat) {
		return exit(c.ui, "invalid format type (expected: json|yaml|tabular)")
	}

	cmdArgs := c.flagset.Args()
	if a := len(cmdArgs); a != 2 {
		if a == 0 {
			return clui.ExitCode{}
		}
		return exit(c.ui, "missing required arguments (expected: 2)")
	}

	database := cmdArgs[0]
	if !contains([]string{"local", "global"}, database) {
		return exit(c.ui, "invalid database type (expected: local|global)")
	}

	var bits []byte
	if cmdArgs[1] == "-" {
		var err error
		if bits, err = ioutil.ReadAll(os.Stdin); err != nil {
			return exit(c.ui, err.Error())
		}
	} else {
		bits = []byte(cmdArgs[1])
	}

	var queries Queries
	if err := yaml.Unmarshal(bits, &queries); err != nil {
		return exit(c.ui, err.Error())
	}

	client, err := getClient(c.address, certs{
		serverCert: c.serverCert,
		clientCert: c.clientCert,
		clientKey:  c.clientKey,
	}, askPassword(c.UI()), logger)
	if err != nil {
		return exit(c.ui, err.Error())
	}

	g := exec.NewGroup()
	exec.Block(g)
	{
		g.Add(func() error {
			data := query.Query{
				Database: database,
				Query:    strings.Join(queries.Queries, ";"),
			}
			response, _, err := client.Query("POST", "/internal/query", data, "")
			if err != nil {
				return errors.Wrap(err, "error requesting")
			}

			var batch query.Batch
			if err := json.Unmarshal(response.Metadata, &batch); err != nil {
				return errors.Wrap(err, "error parsing result")
			}

			for i, result := range batch.Results {
				if len(batch.Results) > 1 {
					c.ui.Output(fmt.Sprintf("=> Query %d:\n", i))
				}
				if result.Type == "select" {
					out := newUIWriterLogger(c.ui)
					renderQueryResult(result, out)
					out.Flush()
				} else {
					c.ui.Output(fmt.Sprintf("Rows affected: %d\n", result.RowsAffected))
				}
			}
			return nil
		}, func(err error) {
			// ignore
		})
	}
	exec.Interrupt(g)
	if err := g.Run(); err != nil {
		return exit(c.ui, err.Error())
	}

	return clui.ExitCode{}
}

// Queries represents all the different queries you can send to the query
// command
type Queries struct {
	Queries []string `json:"queries" yaml:"queries"`
}

type uiWriterLogger struct {
	buf *bytes.Buffer
	ui  clui.UI
}

func newUIWriterLogger(ui clui.UI) *uiWriterLogger {
	return &uiWriterLogger{
		buf: new(bytes.Buffer),
		ui:  ui,
	}
}

func (w *uiWriterLogger) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *uiWriterLogger) Flush() {
	w.ui.Output(w.buf.String())
}

func renderQueryResult(r query.Result, out io.Writer) {
	table := tablewriter.NewWriter(out)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	table.SetHeader(r.Columns)
	for _, row := range r.Rows {
		data := []string{}
		for _, col := range row {
			data = append(data, fmt.Sprintf("%v", col))
		}
		table.Append(data)
	}
	table.Render()
}

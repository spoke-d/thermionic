package clui

import (
	"bytes"
	"fmt"
	"text/tabwriter"
)

// helpTemplateData defines the template data that can be expressed when
// creating the commands help templates.
type helpTemplateData struct {
	Name        string
	Help        string
	SubCommands []subHelpTemplateData
}

type subHelpTemplateData struct {
	Name        string
	NameAligned string
	Help        string
	Synopsis    string
}

type helpTemplate struct {
	tab *tabwriter.Writer
	buf *bytes.Buffer
	ui  UI
}

func newHelpTemplate(ui UI, command Command) (*helpTemplate, error) {
	buf := new(bytes.Buffer)
	return &helpTemplate{
		tab: tabwriter.NewWriter(buf, 2, 2, 2, ' ', 0),
		buf: buf,
		ui:  ui,
	}, nil
}

func (h *helpTemplate) Render(data helpTemplateData) error {
	h.ui.Output(data.Help)
	if len(data.SubCommands) > 0 {
		h.ui.Output("Subcommands:\n")
		for _, v := range data.SubCommands {
			h.tab.Write([]byte(fmt.Sprintf("  %s\t%s\t\n", v.Name, v.Synopsis)))
		}
		if err := h.tab.Flush(); err != nil {
			return nil
		}
		h.ui.Info(h.buf.String())
	}
	return nil
}

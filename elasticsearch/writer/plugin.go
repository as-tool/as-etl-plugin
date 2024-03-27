package elasticsearch

import (
	"github.com/as-tool/as-etl-engine/core/plugin/writer"

	"github.com/as-tool/as-etl-engine/common/config"
)

func RegistPlugin() {
	var err error
	maker := &Maker{}
	if err = writer.RegisterWriter(maker); err != nil {
		panic(err)
	}
}

var pluginConfig = `{
	"name" : "elasticsearchwriter",
	"developer":"allen",
	"dialect":"elastic",
	"description":""
}`

// NewWriterFromString create writer
func NewWriterFromString(plugin string) (wr writer.Writer, err error) {
	w := &Writer{}
	if w.pluginConf, err = config.NewJSONFromString(plugin); err != nil {
		return nil, err
	}
	wr = w
	return
}

type Maker struct{}

func (m *Maker) Default() (writer.Writer, error) {
	return NewWriterFromString(pluginConfig)
}

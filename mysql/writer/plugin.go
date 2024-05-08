package mysql

import (
	"github.com/as-tool/as-etl-engine/common/config"
	"github.com/as-tool/as-etl-engine/core/plugin/writer"
)

func RegistPlugin() {
	var err error
	maker := &maker{}
	if err = writer.RegisterWriter(maker); err != nil {
		panic(err)
	}
}

var pluginConfig = `{
    "name" : "mysqlwriter",
    "developer":"Breeze0806",
    "dialect":"mysql",
    "description":"use github.com/go-sql-driver/mysql. database/sql DB execute select sql, retrieve data from the ResultSet. warn: The more you know about the database, the less problems you encounter."
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

type maker struct{}

func (m *maker) Default() (writer.Writer, error) {
	return NewWriterFromString(pluginConfig)
}

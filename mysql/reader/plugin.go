package mysql

import (
	"github.com/as-tool/as-etl-engine/common/config"
	"github.com/as-tool/as-etl-engine/core/plugin/reader"
)

func RegistPlugin() {
	var err error
	maker := &maker{}
	if err = reader.RegisterReader(maker); err != nil {
		panic(err)
	}
}

var pluginConfig = `{
    "name" : "mysqlreader",
    "developer":"Breeze0806",
    "dialect":"mysql",
    "description":"use github.com/go-sql-driver/mysql. database/sql DB execute select sql, retrieve data from the ResultSet. warn: The more you know about the database, the less problems you encounter."
}`

// NewReaderFromString create reader
func NewReaderFromString(plugin string) (rd reader.Reader, err error) {
	r := &Reader{}
	if r.pluginConf, err = config.NewJSONFromString(plugin); err != nil {
		return nil, err
	}
	rd = r
	return
}

type maker struct{}

func (m *maker) Default() (reader.Reader, error) {
	return NewReaderFromString(pluginConfig)
}

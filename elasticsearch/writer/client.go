package elasticsearch

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/as-tool/as-etl-engine/common/config"
	"github.com/olivere/elastic/v7"
)

func ES_init(conf *config.JSON) *elastic.Client {
	username := GetUsername(conf)
	pass := GetPassword(conf)
	url := GetEndpoint(conf)
	es, err := elastic.NewClient(elastic.SetURL("http://"+url), elastic.SetBasicAuth(username, pass))
	if err != nil {
		msg := fmt.Sprintf("Error creating the client: %s", err)
		fmt.Println(msg)
		slog.Error(msg)
		os.Exit(1)
	}

	return es
}

func ES_Version(client *elastic.Client, conf *config.JSON) string {
	url := GetEndpoint(conf)
	es_version, _ := client.ElasticsearchVersion("http://" + url)
	return es_version
}

func ES_close() {

}

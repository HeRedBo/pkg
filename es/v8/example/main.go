package main

import (
	"context"
	"log"
	v8 "pkg/es/v8"

	"github.com/gookit/goutil/dump"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

var indexCreateJson = `
{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1
  },
  "mappings": {
  "dynamic": "strict",
    "properties": {
      "id": {
        "type": "keyword",
        "doc_values": false,
        "norms": false,
        "similarity": "boolean"
      },
      "name": {
        "type": "text"
      },
      "age":{
        "type": "short"
      }
    }
  }
}
`

func main() {

	indexName := "user"
	ctx := context.Background()
	err := v8.InitClient(v8.DefaultClient, []string{"http://127.0.0.1:9200"}, "elastic", "elastic")
	if err != nil {
		log.Println(err)
		return
	}

	esClient := v8.GetClient(v8.DefaultClient)

	// create index
	err = esClient.CreateIndex(ctx, indexName, indexCreateJson, true)
	if err != nil {
		dump.Println(err)
	}

}

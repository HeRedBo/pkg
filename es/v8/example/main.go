package main

import (
	"context"
	"encoding/json"
	v8 "github.com/HeRedBo/pkg/es/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/scroll"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/gookit/goutil/dump"
	"log"
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

type User struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

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
	//err = esClient.CreateIndex(ctx, indexName, indexCreateJson, true)
	//if err != nil {
	//	dump.Println(err)
	//}

	// delete index
	//res, err := esClient.DeleteIndex(ctx, indexName)
	//if err != nil {
	//	dump.Println(res, err)
	//	return
	//}

	// insert data
	//user := User{
	//	Id:   1,
	//	Name: "jack ma",
	//	Age:  25,
	//}
	//res, err := esClient.Insert(ctx, indexName, strconv.FormatInt(user.Id, 10), strconv.FormatInt(user.Id, 10), v8.OpTypeIndex, user)
	//if err != nil {
	//	dump.Println(res, err)
	//	return
	//}
	//
	//dump.Println(res, err)
	//return

	// update data
	//userForUpdate := map[string]interface{}{
	//	"name": "update jack ma",
	//	"age":  25,
	//}
	//
	//res, err := esClient.Update(ctx, indexName, "1", "1", userForUpdate)
	//if err != nil {
	//	dump.Println(err, res)
	//}
	//
	//dump.P(res, err)
	//return
	//
	//user2 := User{
	//	Id:   3,
	//	Name: "haha tim",
	//	Age:  35,
	//}
	//
	//jsonDoc, err := json.Marshal(user2)
	//err = esClient.BulkInsert(ctx, indexName, strconv.FormatInt(user2.Id, 10), strconv.FormatInt(user2.Id, 10), string(jsonDoc), func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error) {
	//	if err != nil {
	//		dump.Println(err, item2)
	//	}
	//})
	//if err != nil {
	//	dump.Println(err)
	//}

	//user2ForBulkUpdate := map[string]interface{}{
	//	"name": "update tim jerry",
	//	"age":  50,
	//}
	//
	//err = esClient.BulkUpdate(ctx, indexName, strconv.FormatInt(user2.Id, 10), strconv.FormatInt(user2.Id, 10), user2ForBulkUpdate, func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error) {
	//	if err != nil {
	//		log.Println(err, item2)
	//	}
	//})
	//if err != nil {
	//	log.Println(err)
	//}

	//err = esClient.Close(ctx)
	//stats := esClient.BulkProcessor.Stats()
	//log.Printf("已提交: %d, 失败: %d", stats.NumAdded, stats.NumFailed)
	//
	//query := &types.Query{
	//	Term: map[string]types.TermQuery{
	//		"age": {Value: 30},
	//	},
	//}
	//script := map[string]interface{}{
	//	"inline": "ctx._source.name = params.name",
	//	"params": map[string]interface{}{
	//		"name": "update-by-script",
	//	},
	//}

	// 2. 定义脚本（使用 types.Script 结构体）
	//scriptSource := "ctx._source.name = params.name"
	//script := &types.Script{
	//	Source: &scriptSource, // 关键字段是 Source（不是 Inline）
	//	Params: map[string]json.RawMessage{
	//		"name": json.RawMessage(`"update-by-script"`), // 直接传递 JSON 字符串的字节
	//		// 复杂参数示例：
	//		//"metadata": json.RawMessage(`{"priority": 5, "author": "Alice"}`),
	//	},
	//}
	//res, err := esClient.UpdateByQuery(ctx, indexName, "2", "2", query, script)
	//if err != nil {
	//	log.Println(err, res)
	//	return
	//}

	//query := &types.Query{
	//	Bool: &types.BoolQuery{
	//		Must: []types.Query{
	//			{
	//				Term: map[string]types.TermQuery{
	//					"age": {Value: 25},
	//				},
	//			},
	//			//{
	//			//	MatchPhrase: map[string]types.MatchPhraseQuery{
	//			//		"name": {Query: "jerry"},
	//			//	},
	//			//},
	//		},
	//	},
	//}

	//
	//res, err := esClient.Query(ctx, indexName, "1", query, 0, 10, nil)
	//if err != nil {
	//	log.Println(err)
	//	return
	//}
	//
	//log.Println(res.Hits.Total.Value)
	//for _, hit := range res.Hits.Hits {
	//	user := User{}
	//	err = json.Unmarshal(hit.Source_, &user)
	//	if err != nil {
	//		log.Println(err)
	//		return
	//	}
	//	dump.Println(user)
	//	//log.Println(user)
	//}

	query := &types.Query{
		MatchAll: &types.MatchAllQuery{},
	}

	//query := &types.Query{
	//	Term: map[string]types.TermQuery{
	//		"age": {Value: 30},
	//	},
	//}

	err = esClient.ScrollQuery(ctx, indexName, "", query, 2, func(res *scroll.Response, err error) {
		if err != nil {
			log.Println(err)
			return
		}
		log.Println(res.Hits.Total.Value)
		for _, hit := range res.Hits.Hits {
			user := User{}
			err = json.Unmarshal(hit.Source_, &user)
			if err != nil {
				//log.Println(err)
				dump.Println(err)
				return
			}
			dump.Println(user)
			//log.Println(user)
		}
	})
	if err != nil {
		log.Println(err)
	}

	//dump.Println(res)

}

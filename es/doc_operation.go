package es

import (
	"context"
	"github.com/olivere/elastic/v7"
)

const (
	DefaultClient       = "es-default-client"
	DefaultReaDClient   = "es-default-read-client"
	DefaultWriteClient  = "es-default-write-client"
	DefaultVersionType  = "external"
	VersionTypeInternal = "internal"
	DefaultRefresh      = "false"
	RefreshWaiterFor    = "wait_for"
	RefreshTrue         = "true"
	DefaultScriptLang   = "painless"
)

type BulkDoc struct {
	ID          string
	Routing     string
	Version     int64
	VersionType string
}

type BulkCreateDoc struct {
	BulkDoc
	Doc interface{}
}

type BulkUpsertDoc struct {
	BulkDoc
	update map[string]interface{}
}

type BulkUpdateDoc struct {
	BulkDoc
	update map[string]interface{}
}

func (c *Client) Create(ctx context.Context, indexName, id, routing string, doc interface{}) error {
	//注意 sdk这里第一个index获取的是*IndexService，即索引服务，第二个index是指定需要写入的索引名
	indexService := c.Client.Index().Index(indexName).OpType("create")
	if len(id) > 0 {
		indexService.Id(id)
	}
	if len(routing) > 0 {
		indexService.Routing(routing)
	}
	_, err := indexService.BodyJson(doc).Refresh(DefaultRefresh).Do(ctx)
	return err
}

// BulkCreate 批量的方式新建文档，后台提交
func (c *Client) BulkCreate(indexName, id, routing string, doc interface{}) {
	bulkCreateRequest := elastic.NewBulkCreateRequest().Index(indexName).Doc(doc)
	if len(id) > 0 {
		bulkCreateRequest.Id(id)
	}
	if len(routing) > 0 {
		bulkCreateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkCreateRequest)
}

func (c *Client) BulkCreateDocs(ctx context.Context, indexName string, docs []*BulkCreateDoc) (*elastic.BulkResponse, error) {
	bulkService := c.Client.Bulk().ErrorTrace(true)
	for _, doc := range docs {
		bulkCreateRequest := elastic.NewBulkCreateRequest().Index(indexName)
		if len(doc.ID) > 0 {
			bulkCreateRequest.Id(doc.ID)
		}
		if len(doc.Routing) > 0 {
			bulkCreateRequest.Routing(doc.Routing)
		}
		bulkService.Add(bulkCreateRequest)
	}
	return bulkService.Do(ctx)
}

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

func (c *Client) BulkReplace(indexName, id, routing string, doc interface{}) {
	bulkCreateRequest := elastic.NewBulkIndexRequest().Index(indexName).Doc(doc)
	if len(id) > 0 {
		bulkCreateRequest.Id(routing)
	}
	if len(routing) > 0 {
		bulkCreateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkCreateRequest)
}

func (c *Client) BulkReplaceDocs(ctx context.Context, indexName string, docs []*BulkCreateDoc) (*elastic.BulkResponse, error) {
	bulkService := c.Client.Bulk().ErrorTrace(true)
	for _, doc := range docs {
		bulkCreateRequest := elastic.NewBulkIndexRequest().Index(indexName)
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

//业务数据修改时生成版本号，在更新ES文档数据时，根据这个版本号来避免重复和乱序处理，实现操作的幂等性
//PUT twitter/_doc/1?version=2&version_type=external
//{
//"message" : "elasticsearch now has versioning support, double cool!"
//}
//1.版本管理是完全实时的，不受搜索操作的近实时方面的影响
//2.如果当前提供的版本号比实际的版本号大（或者实际文档不存在）就会成功写入，并设置为当前的版本号，否则返回失败（409状态码）

func (c *Client) BulkCreateWithVersion(ctx context.Context, indexName, id, routing string, version int64, doc interface{}) {
	bulkCreateRequest := elastic.NewBulkIndexRequest().Index(indexName).Doc(doc).VersionType(DefaultVersionType).Version(version)
	if len(id) > 0 {
		bulkCreateRequest.Id(routing)
	}
	if len(routing) > 0 {
		bulkCreateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkCreateRequest)
}

func (c *Client) Delete(ctx context.Context, indexName, id, routing string) error {
	deleteService := c.Client.Delete().Index(indexName).Id(id).Refresh(DefaultRefresh)
	if len(routing) > 0 {
		deleteService.Routing(routing)
	}
	_, err := deleteService.Do(ctx)
	return err
}

func (c *Client) DeleteRefresh(ctx context.Context, indexName, id, routing string) error {
	
}

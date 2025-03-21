package v8

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/index"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/optype"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
	"strings"
)

const (
	DefaultVersionType  = "external"
	VersionTypeInternal = "internal"
	DefaultRefresh      = "false"
	RefreshWaitFor      = "wait_for"
	RefreshTrue         = "true"
	DefaultScriptLang   = "painless"
	Bulk
	OpTypeCreate = "create"
	OpTypeIndex  = "index"
	OpTypeUpsert = "upsert"
	OpTypeUpdate = "update"
	OpTypeDelete = "delete"
)

type BulkIndexerOnFailure func(context.Context, esutil.BulkIndexerItem, esutil.BulkIndexerResponseItem, error)

// Insert 新建文档
func (c *Client) Insert(ctx context.Context, indexName, id, routing, action string, doc interface{}) (*index.Response, error) {
	OpType := optype.OpType{Name: action}
	indexService := c.Client.Index(indexName).OpType(OpType)
	if len(id) > 0 {
		indexService.Id(id)
	}
	if len(routing) > 0 {
		indexService.Routing(routing)
	}
	//Refresh setting
	//false 不采取任何与刷新有关的行动。这个请求所做的改变将在请求返回后的某个时间点变得可见
	//true; 在操作发生后立即刷新相关的主分片和副本分片（而不是整个索引），以便更新的文档立即出现在搜索结果中.对性能影响最大
	//wait_for 在操作响应之前，等待请求所做的改变通过刷新而变得可见。这并不强迫立即进行刷新，而是等待刷新的发生。
	//Elasticsearch每隔index.refresh_interval（默认为一秒）就会自动刷新发生变化的分片
	return indexService.Request(doc).Refresh(refresh.False).Do(ctx)
}

// BulkInsert 批量的方式新建文档，后台提交
func (c *Client) BulkInsert(ctx context.Context, indexName, id, routing string, jsonDoc string, onFailure BulkIndexerOnFailure) error {
	bulkIndexerItem := esutil.BulkIndexerItem{}
	bulkIndexerItem.Index = indexName
	if len(id) > 0 {
		bulkIndexerItem.DocumentID = id
	}
	if len(routing) > 0 {
		bulkIndexerItem.Routing = routing
	}
	bulkIndexerItem.Action = OpTypeCreate
	bulkIndexerItem.RetryOnConflict = esapi.IntPtr(3)
	bulkIndexerItem.Body = strings.NewReader(jsonDoc)
	bulkIndexerItem.OnFailure = onFailure

	return c.BulkProcessor.Add(ctx, bulkIndexerItem)
}

// BulkIndex 批量的方式新建文档(覆盖写)，后台提交
func (c *Client) BulkIndex(ctx context.Context, indexName, id, routing, jsonDoc string, onFailure BulkIndexerOnFailure) error {
	bulkIndexerItem := esutil.BulkIndexerItem{}
	bulkIndexerItem.Index = indexName
	if len(id) > 0 {
		bulkIndexerItem.DocumentID = id
	}
	if len(routing) > 0 {
		bulkIndexerItem.Routing = routing
	}
	bulkIndexerItem.Action = OpTypeIndex
	bulkIndexerItem.RetryOnConflict = esapi.IntPtr(3)
	bulkIndexerItem.Body = strings.NewReader(jsonDoc)
	bulkIndexerItem.OnFailure = onFailure

	return c.BulkProcessor.Add(ctx, bulkIndexerItem)
}

func (c *Client) BulkUpdate(ctx context.Context, indexName, id, routing string, update map[string]interface{}, onFailure BulkIndexerOnFailure) error {
	if len(id) == 0 {
		return errors.New("_doc id is required")
	}
	updateDoc := map[string]interface{}{
		"doc": update,
	}
	jsonDoc, err := json.Marshal(updateDoc)
	if err != nil {
		return err
	}
	bulkIndexerItem := esutil.BulkIndexerItem{}
	bulkIndexerItem.Index = indexName
	bulkIndexerItem.DocumentID = id
	bulkIndexerItem.Body = strings.NewReader(string(jsonDoc))
	if len(routing) > 0 {
		bulkIndexerItem.Routing = routing
	}
	bulkIndexerItem.Action = OpTypeIndex
	bulkIndexerItem.RetryOnConflict = esapi.IntPtr(3)
	bulkIndexerItem.OnFailure = onFailure
	return c.BulkProcessor.Add(ctx, bulkIndexerItem)
}

func (c *Client) BulkInsertWithSeqNo(ctx context.Context, id, routing, indexName, action, jsonDoc string, seqNo, primaryTerm *int64, onFailure BulkIndexerOnFailure) error {
	bulkIndexerItem := esutil.BulkIndexerItem{}
	bulkIndexerItem.Index = indexName

	if len(id) > 0 {
		bulkIndexerItem.DocumentID = id
	}
	if len(routing) > 0 {
		bulkIndexerItem.Routing = routing
	}
	bulkIndexerItem.Action = action
	bulkIndexerItem.IfSeqNo = seqNo
	bulkIndexerItem.IfPrimaryTerm = primaryTerm
	bulkIndexerItem.VersionType = DefaultVersionType
	bulkIndexerItem.Body = strings.NewReader(jsonDoc)
	bulkIndexerItem.OnFailure = onFailure
	return c.BulkProcessor.Add(ctx, bulkIndexerItem)
}

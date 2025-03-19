package v8

import (
	"context"
	"strings"
)

// IndexExists 检查索引是否存在
func (c *Client) IndexExists(ctx context.Context, indexName string, forceCheck bool) (bool, error) {
	if !forceCheck {
		if _, ok := c.CacheIndices.Load(indexName); ok {
			return true, nil
		}
	}

	//在ES中可以同时校验多个索引是否存在，校验多个索引时，
	//只要有一个索引不存在，就会返回false，实际场景很少会用到，这里直接校验单索引
	exists, err := c.Client.Indices.Exists(indexName).Do(ctx)
	if exists {
		c.CacheIndices.Store(indexName, true)
	}
	return exists, err
}

// CreateIndex 创建索引
func (c *Client) CreateIndex(ctx context.Context, indexName string, bodyJson string, forceCheck bool) error {
	//创建索引比较耗时，创建过程中防止其他创建请求过来，这里可以加锁处理
	c.lock.Lock()
	defer c.lock.Unlock()
	exists, err := c.IndexExists(ctx, indexName, forceCheck)
	if err != nil {
		return err
	}
	// 索引已创建
	if exists {
		return nil
	}
	// 如果重复创建会报错
	_, err = c.Client.Indices.Create(indexName).Raw(strings.NewReader(bodyJson)).Do(ctx)
	if err != nil {
		return err
	}
	c.CacheIndices.Store(indexName, true)
	return nil
}

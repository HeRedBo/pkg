package es

import "context"

// IndexExists 检查索引是否存在
func (c *Client) IndexExists(ctx context.Context, indexName string, forceCheck bool) (bool, error) {
	if !forceCheck {
		if _, ok := c.CacheIndices.Load(indexName); ok {
			return true, nil
		}
	}

	exists, err := c.Client.IndexExists(indexName).Do(ctx)
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
	_, err = c.Client.CreateIndex(indexName).BodyString(bodyJson).Do(ctx)
	if err != nil {
		c.CacheIndices.Store(indexName, true)
	}
	return nil

}

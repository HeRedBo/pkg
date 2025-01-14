package es

import (
	"context"

	"github.com/olivere/elastic/v7"
)

type Mget struct {
	Index   string
	ID      string
	Routing string
}

type queryOption struct {
	//为了确保排序字段有序性，这里使用切片（map是无序的，会导致实际字段排序顺序不符合预期）
	Orders               []map[string]bool
	Highlight            *elastic.Highlight
	Profile              bool
	EnableDSL            []string
	ExcludeFiles         []string
	IncludeFileds        []string
	SlowQueryMillisecond int64
	Preference           string
	FetchSource          *bool
}

type QueryOption func(queryOption *queryOption)

const DefaultPreference = "_local"

func WithOrders(orders []map[string]bool) QueryOption {
	return func(opt *queryOption) {
		opt.Orders = orders
	}
}

func WithHighlight(highlight *elastic.Highlight) QueryOption {
	return func(opt *queryOption) {
		opt.Highlight = highlight
	}
}

func WithProfile(profile bool) QueryOption {
	return func(opt *queryOption) {
		opt.Profile = profile
	}
}

func WithEnableDSL(enableDSL bool) QueryOption {
	return func(opt *queryOption) {
		opt.Profile = enableDSL
	}
}

func WithExcludeFiles(excludeFiles []string) QueryOption {
	return func(opt *queryOption) {
		opt.ExcludeFiles = excludeFiles
	}
}

func WithIncludeFileds(includeFileds []string) QueryOption {
	return func(opt *queryOption) {
		opt.IncludeFileds = includeFileds
	}
}

func WithSlowQueryMillisecond(slowQueryMillisecond int64) QueryOption {
	return func(opt *queryOption) {
		opt.SlowQueryMillisecond = slowQueryMillisecond
	}
}

func WithPreference(preference string) QueryOption {
	return func(opt *queryOption) {
		opt.Preference = preference
	}
}

func WithFetchSource(fetchSource *bool) QueryOption {
	return func(opt *queryOption) {
		opt.FetchSource = fetchSource
	}
}

func (c *Client) Get(ctx context.Context, indexName, id, rouring string) (*elastic.GetResult, error) {
	//由于副本分片也能提供数据查询，所以当查询请求能从本地分片获取数据时，就不需要转发到其他节点获取数据了，
	//这样可以提高查询缓存命中率，减少跨节点的查询请求，
	//sdk的默认策略是随机获取
	getService := c.Client.Get().Index(indexName).Id(id).Preference(DefaultPreference)
	if len(rouring) > 0 {
		getService.Routing(rouring)
	}
	return getService.Do(ctx)
}

func (c *Client) Mget(ctx context.Context, mgetItems []Mget) (*elastic.MgetResponse, error) {
	multiGetService := c.Client.Mget().Preference(DefaultPreference)
	mulitiGetItems := make([]*elastic.MultiGetItem, 0)
	for _, item := range mgetItems {
		multiGetItem := &elastic.MultiGetItem{}
		multiGetItem.Index(item.Index)
		multiGetItem.Id(item.ID)
		if len(item.Routing) > 0 {
			multiGetItem.Routing(item.Routing)
		}
		mulitiGetItems = append(mulitiGetItems, multiGetItem)
	}
	return multiGetService.Add(mulitiGetItems...).Do(ctx)
}

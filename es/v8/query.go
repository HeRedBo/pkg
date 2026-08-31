package v8

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/get"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/mget"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/scroll"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

type Mget struct {
	Index   string
	ID      string
	Routing string
}

type queryOption struct {
	//为了确保排序字段有序性，这里使用切片（map是无序的，会导致实际字段排序顺序不符合预期）
	Orders               []map[string]bool
	Highlight            *types.Highlight
	Profile              bool
	EnableDSL            bool
	FetchSource          *bool
	ExcludeFiles         []string
	IncludeFields        []string
	SlowQueryMillisecond int64
	Preference           string
	Analyzer             string
}

type QueryOption func(queryOption *queryOption)

const DefaultPreference = "_local"

func WithOrders(orders []map[string]bool) QueryOption {
	return func(opt *queryOption) {
		opt.Orders = orders
	}
}

func WithHighlight(highlight *types.Highlight) QueryOption {
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
		opt.EnableDSL = enableDSL
	}
}

func WithExcludeFiles(excludeFiles []string) QueryOption {
	return func(opt *queryOption) {
		opt.ExcludeFiles = excludeFiles
	}
}

func WithIncludeFileds(includeFields []string) QueryOption {
	return func(opt *queryOption) {
		opt.IncludeFields = includeFields
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
func WithAnalyzer(analyzer string) QueryOption {
	return func(opt *queryOption) {
		opt.Analyzer = analyzer
	}
}

func (c *Client) Get(ctx context.Context, indexName, id, routing string, preference string, excludes []string) (*get.Response, error) {
	//由于副本分片也能提供数据查询，所以当查询请求能从本地分片获取数据时，就不需要转发到其他节点获取数据了，
	//这样可以提高查询缓存命中率，减少跨节点的查询请求，
	//sdk的默认策略是随机获取
	if len(id) == 0 {
		return nil, errors.New("_doc is required")
	}
	getService := c.Client.Get(indexName, id)
	if len(routing) > 0 {
		getService.Routing(routing)
	}
	if len(preference) > 0 {
		getService.Preference(preference)
	}
	if len(excludes) > 0 {
		getService.SourceExcludes_(excludes...)
	}
	return getService.Do(ctx)
}

func (c *Client) Mget(ctx context.Context, indexName, routing string, ids, excludes []string) (*mget.Response, error) {
	if len(ids) == 0 {
		return nil, errors.New("_doc is required")
	}
	multiGetService := c.Client.Mget().Index(indexName).Ids(ids...).Preference(DefaultPreference)
	if len(routing) > 0 {
		multiGetService.Routing(routing)
	}
	if len(excludes) > 0 {
		multiGetService.SourceExcludes_(excludes...)
	}
	return multiGetService.Do(ctx)
}

func (c *Client) Query(ctx context.Context, indexName string, routing string, query *types.Query, from, size int, options ...QueryOption) (*search.Response, error) {
	queryOpt := &queryOption{}
	for _, f := range options {
		if f != nil {
			f(queryOpt)
		}
	}
	t := time.Now()
	defer func() {
		if queryOpt.SlowQueryMillisecond > 0 && time.Since(t).Milliseconds() > queryOpt.SlowQueryMillisecond {
			dslJSON := c.buildDSL(query, from, size, queryOpt)
			c.dslLog.Warn("es slow query",
				Field("dsl", dslJSON),
				Field("routing", routing),
				Field("took_ms", time.Since(t).Milliseconds()),
			)
		}
	}()
	searchService := c.Client.Search().Index(indexName).Query(query).AllowPartialSearchResults(true)
	if len(routing) > 0 {
		searchService.Routing(routing)
	}
	if len(queryOpt.Preference) > 0 {
		searchService.Preference(queryOpt.Preference)
	} else {
		searchService.Preference(DefaultPreference)
	}

	if len(queryOpt.Analyzer) > 0 {
		searchService.Analyzer(queryOpt.Analyzer)
	}

	if len(queryOpt.IncludeFields) > 0 {
		searchService.SourceExcludes_(queryOpt.IncludeFields...)
	}
	if len(queryOpt.ExcludeFiles) > 0 {
		searchService.SourceExcludes_(queryOpt.ExcludeFiles...)
	}

	if queryOpt.Highlight != nil {
		searchService.Highlight(queryOpt.Highlight)
	}

	fetchSource := true
	if queryOpt.FetchSource != nil && !*queryOpt.FetchSource {
		fetchSource = false
	}

	searchService.Source_(fetchSource)

	if queryOpt.Profile {
		searchService.Profile(true)
	}

	if len(queryOpt.Orders) > 0 {
		for _, orderM := range queryOpt.Orders {
			for field, order := range orderM {
				searchService.Sort(field, order)
			}
		}
	}
	if from > 0 {
		searchService.From(from)
	}
	if size > 0 {
		searchService.Size(size)
	}

	// DSL 日志输出
	if c.DebugMode || c.QueryLogEnable || queryOpt.EnableDSL {
		dslJSON := c.buildDSL(query, from, size, queryOpt)
		c.dslLog.Info("es query", Field("dsl", dslJSON), Field("routing", routing))
	}

	return searchService.Do(ctx)
}

// buildDSL 构造查询体 JSON 用于日志输出
func (c *Client) buildDSL(query *types.Query, from, size int, queryOpt *queryOption) string {
	dsl := map[string]interface{}{}
	if query != nil {
		dsl["query"] = query
	}
	if from > 0 {
		dsl["from"] = from
	}
	if size > 0 {
		dsl["size"] = size
	}
	if len(queryOpt.Orders) > 0 {
		sortArr := make([]map[string]string, 0, len(queryOpt.Orders))
		for _, orderM := range queryOpt.Orders {
			for field, order := range orderM {
				dir := "asc"
				if !order {
					dir = "desc"
				}
				sortArr = append(sortArr, map[string]string{field: dir})
			}
		}
		dsl["sort"] = sortArr
	}
	if queryOpt.Highlight != nil {
		dsl["highlight"] = queryOpt.Highlight
	}
	if queryOpt.Profile {
		dsl["profile"] = true
	}
	data, _ := json.Marshal(dsl)
	return string(data)
}

func (c *Client) ScrollQuery(ctx context.Context, indexName, routing string, query *types.Query, size int, callback func(res *scroll.Response, err error), options ...QueryOption) error {
	queryOpt := &queryOption{}

	for _, f := range options {
		if f != nil {
			f(queryOpt)
		}
	}

	searchService := c.Client.Search().Index(indexName).Query(query).AllowPartialSearchResults(true)
	if len(routing) > 0 {
		searchService.Routing(routing)
	}
	if len(queryOpt.Preference) > 0 {
		searchService.Preference(queryOpt.Preference)
	} else {
		searchService.Preference(DefaultPreference)
	}

	if len(queryOpt.Analyzer) > 0 {
		searchService.Analyzer(queryOpt.Analyzer)
	}

	if len(queryOpt.IncludeFields) > 0 {
		searchService.SourceExcludes_(queryOpt.IncludeFields...)
	}
	if len(queryOpt.ExcludeFiles) > 0 {
		searchService.SourceExcludes_(queryOpt.ExcludeFiles...)
	}

	if queryOpt.Highlight != nil {
		searchService.Highlight(queryOpt.Highlight)
	}

	fetchSource := true
	if queryOpt.FetchSource != nil && !*queryOpt.FetchSource {
		fetchSource = false
	}

	searchService.Source_(fetchSource)

	if queryOpt.Profile {
		searchService.Profile(true)
	}

	if len(queryOpt.Orders) > 0 {
		for _, orderM := range queryOpt.Orders {
			for field, order := range orderM {
				searchService.Sort(field, order)
			}
		}
	}
	res, err := searchService.Scroll("1m").Do(ctx)
	scrollRes := &scroll.Response{
		Aggregations:    res.Aggregations,
		Clusters_:       res.Clusters_,
		Fields:          res.Fields,
		Hits:            res.Hits,
		MaxScore:        res.MaxScore,
		NumReducePhases: res.NumReducePhases,
		PitId:           res.PitId,
		ScrollId_:       res.ScrollId_,
		Shards_:         res.Shards_,
		Suggest:         res.Suggest,
		TimedOut:        res.TimedOut,
		Took:            res.Took,
	}

	callback(scrollRes, err)
	if err != nil {
		return err
	}

	scrollID := *res.ScrollId_

	// 使用 Scroll API 获取剩余结果
	for {
		result, err := c.Client.Scroll().Scroll("1m").ScrollId(scrollID).Do(ctx)
		callback(result, err)
		//执行上面的scroll后会返回一个新的scrollId，旧的scrollId需要清除掉

		if err != nil {
			c.log.Error("cannot execute scroll query", ErrField(err))
			return fmt.Errorf("scroll query failed: %w", err)
		}
		if len(result.Hits.Hits) == 0 {
			break
		}

		currentScrollId := *result.ScrollId_
		//清除掉旧的scrollId
		if currentScrollId != *result.ScrollId_ {
			r, e := c.Client.ClearScroll().ScrollId(scrollID).Do(ctx)
			if e != nil {
				c.log.Error("clear scroll context failed", ErrField(e), Field("response", r))
			}
		}
		//更新scrollId，下次请求需要带上这个scrollId，以便继续获取剩余结果
		scrollID = currentScrollId
	}
	return nil

}

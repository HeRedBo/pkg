package v8

import (
	"context"
	"errors"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/get"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/mget"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/scroll"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"log"
	"time"
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
	IncludeFileds        []string
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
func WithAnalyzer(analyzer string) QueryOption {
	return func(opt *queryOption) {
		opt.Analyzer = analyzer
	}
}

func (c *Client) Get(ctx context.Context, indexName, id, rouring string, preference string, excludes []string) (*get.Response, error) {
	//由于副本分片也能提供数据查询，所以当查询请求能从本地分片获取数据时，就不需要转发到其他节点获取数据了，
	//这样可以提高查询缓存命中率，减少跨节点的查询请求，
	//sdk的默认策略是随机获取
	if len(id) == 0 {
		return nil, errors.New("_doc is required")
	}
	getService := c.Client.Get(indexName, id)
	if len(rouring) > 0 {
		getService.Routing(rouring)
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
			log.Println("query slow query", query, "queryOpt", queryOpt)
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

	if len(queryOpt.IncludeFileds) > 0 {
		searchService.SourceExcludes_(queryOpt.IncludeFileds...)
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
	return searchService.Do(ctx)
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

	if len(queryOpt.IncludeFileds) > 0 {
		searchService.SourceExcludes_(queryOpt.IncludeFileds...)
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
	res, err := searchService.Scroll("lm").Do(ctx)
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
	//b, e := res.Hits.Hits[0].Source_.MarshalJSON()
	//log.Println(string(b), e)

	// 使用 Scroll API 获取剩余结果
	for {
		//log.Println(scrollID)
		result, err := c.Client.Scroll().Scroll("1m").ScrollId(scrollID).Do(ctx)
		callback(result, err)
		//执行上面的scroll后会返回一个新的scrollId，旧的scrollId需要清除掉

		//b, e := result.Hits.Hits[0].Source_.MarshalJSON()
		//log.Println(string(b), e)
		if err != nil {
			log.Fatalf("Cannot execute scroll query:%s", err)
			return err
		}
		if len(result.Hits.Hits) == 0 {
			break
		}

		currentScrollId := *result.ScrollId_
		//清除掉旧的scrollId
		if currentScrollId != *result.ScrollId_ {
			r, e := c.Client.ClearScroll().ScrollId(scrollID).Do(ctx)
			if e != nil {
				log.Println(r, e)
			}
		}
		//更新scrollId，下次请求需要带上这个scrollId，以便继续获取剩余结果
		scrollID = currentScrollId
	}
	return nil

}

package es

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/olivere/elastic/v7"

	"github.com/HeRedBo/pkg/logx"
)

var clients map[string]*Client

type option struct {
	QueryLogEnable              bool
	GlobalSlowQueryMillisencond int64
	Bulk                        *Bulk
	DebugMode                   bool
	Scheme                      string
	EnableDSL                   bool
	logger                      logx.Logger // 业务日志
	dslLogger                   logx.Logger // DSL 日志（可与业务日志分离）
}

type Option func(*option)

type Client struct {
	Name           string
	Urls           []string
	QueryLogEnable bool
	Username       string
	password       string
	Bulk           *Bulk
	Client         *elastic.Client
	BulkProcessor  *elastic.BulkProcessor
	DebugMode      bool
	CacheIndices   sync.Map
	lock           sync.Mutex
	log            logx.Logger // 业务日志
	dslLog         logx.Logger // DSL 日志
}

type Bulk struct {
	Name          string
	Workers       int
	FlushInterval time.Duration
	ActionSize    int //每批提交的文档数
	RequestSize   int //每批提交的文档大小
	AfterFunc     elastic.BulkAfterFunc
	Ctx           context.Context
}

const (
	SimpleClient = "simple-es-client"
)

func WithQueryLogEnable(enble bool) Option {
	return func(opt *option) {
		opt.QueryLogEnable = enble
	}
}

func WithScheme(scheme string) Option {
	return func(opt *option) {
		opt.Scheme = scheme
	}
}

func WithBulk(bulk *Bulk) Option {
	return func(opt *option) {
		opt.Bulk = bulk
	}
}

func WithDebugMode(debug bool) Option {
	return func(opt *option) {
		opt.DebugMode = debug
	}
}

// WithLogger 注入业务日志 Logger
func WithLogger(l logx.Logger) Option {
	return func(opt *option) {
		opt.logger = l
	}
}

// WithDSLLogger 注入 DSL 日志 Logger（可与业务日志分离，将查询日志输出到不同文件）
func WithDSLLogger(l logx.Logger) Option {
	return func(opt *option) {
		opt.dslLogger = l
	}
}

func getBaseOptions(username, password string, urls ...string) []elastic.ClientOptionFunc {
	options := make([]elastic.ClientOptionFunc, 0)
	options = append(options, elastic.SetURL(urls...))
	options = append(options, elastic.SetBasicAuth(username, password))
	options = append(options, elastic.SetHealthcheckTimeout(15*time.Second))
	//开启Sniff，SDK会定期(默认15分钟一次)嗅探集群中全部节点，将全部节点都加入到连接列表中，
	//后续新增的节点也会自动加入到可连接列表，但实际生产中我们可能会设置专门的协调节点，所以默认不开启嗅探
	options = append(options, elastic.SetSniff(false))
	return options
}

func InitClinet(clientName string, urls []string, username, password string) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}

	log := getLogger(nil)

	client := &Client{
		Urls:           urls,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		Bulk:           DefaultBulk(),
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
		log:            log,
		dslLog:         log,
	}
	client.Bulk.Name = clientName
	options := getBaseOptions(username, password, urls...)
	options = append(options, elastic.SetErrorLog(&elasticLogger{l: log}))
	err := client.newClient(options)
	if err != nil {
		return err
	}
	clients[clientName] = client
	return nil
}

func InitSimpleClient(urls []string, username, password string, options ...Option) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}

	opt := &option{}
	for _, f := range options {
		if f != nil {
			f(opt)
		}
	}

	log := getLogger(opt.logger)
	dslLog := getLogger(opt.dslLogger)

	esOptions := getBaseOptions(username, password, urls...)
	esOptions = append(esOptions, elastic.SetErrorLog(&elasticLogger{l: log}))

	esClient, err := elastic.NewSimpleClient(esOptions...)
	if err != nil {
		return err
	}

	clitent := &Client{
		Name:           SimpleClient,
		Urls:           urls,
		QueryLogEnable: opt.QueryLogEnable,
		Username:       username,
		password:       password,
		Bulk:           opt.Bulk,
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
		Client:         esClient,
		log:            log,
		dslLog:         dslLog,
	}
	if clitent.Bulk == nil {
		clitent.Bulk = DefaultBulk()
	}
	clitent.Bulk.Name = clitent.Name

	if clitent.Bulk.AfterFunc == nil {
		clientLog := log
		clitent.Bulk.AfterFunc = func(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
			if err != nil || (response != nil && response.Errors) {
				res, _ := json.Marshal(response)
				clientLog.Error("bulk execution error",
					logx.Field("executionId", executionId),
					logx.Field("response", string(res)),
					logx.ErrField(err),
				)
			}
		}
	}

	clitent.BulkProcessor, err = esClient.BulkProcessor().
		Name(clitent.Bulk.Name).
		Workers(clitent.Bulk.Workers).
		BulkSize(clitent.Bulk.ActionSize).
		BulkSize(clitent.Bulk.RequestSize).
		FlushInterval(clitent.Bulk.FlushInterval).
		Stats(true).
		After(clitent.Bulk.AfterFunc).
		Do(clitent.Bulk.Ctx)
	if err != nil {
		log.Error("init bulkProcessor error", logx.ErrField(err))
	}

	clients[SimpleClient] = clitent
	return nil
}

func InitClientWithOptions(clientName string, urls []string, username, password string, options ...Option) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}

	opt := &option{}
	for _, f := range options {
		if f != nil {
			f(opt)
		}
	}

	log := getLogger(opt.logger)
	dslLog := getLogger(opt.dslLogger)

	client := &Client{
		Urls:           urls,
		QueryLogEnable: opt.QueryLogEnable,
		Username:       username,
		password:       password,
		Bulk:           opt.Bulk,
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
		DebugMode:      opt.DebugMode,
		log:            log,
		dslLog:         dslLog,
	}

	esOptions := getBaseOptions(username, password, urls...)
	esOptions = append(esOptions, elastic.SetErrorLog(&elasticLogger{l: log}))
	if opt.DebugMode {
		esOptions = append(esOptions, elastic.SetInfoLog(&elasticLogger{l: log}))
	}
	if len(opt.Scheme) > 0 {
		esOptions = append(esOptions, elastic.SetScheme(opt.Scheme))
		esOptions = append(esOptions, elastic.SetHttpClient(getDefaultClient()))
		esOptions = append(esOptions, elastic.SetHealthcheck(false))
	}

	client.QueryLogEnable = opt.QueryLogEnable
	client.Bulk = opt.Bulk
	if client.Bulk == nil {
		client.Bulk = DefaultBulk()
	}
	err := client.newClient(esOptions)
	if err != nil {
		return err
	}
	clients[clientName] = client
	return nil
}

func getDefaultClient() *http.Client {
	tr := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	}
	return &http.Client{Transport: tr}
}

func GetClient(name string) *Client {
	if client, exist := clients[name]; exist {
		return client
	}
	logx.GetLogger().Warn("call init before", logx.Field("client", name))
	return nil
}

func GetSimpleClient() *Client {
	if client, exist := clients[SimpleClient]; exist {
		return client
	}
	logx.GetLogger().Warn("call init before", logx.Field("client", SimpleClient))
	return nil
}

func (c *Client) newClient(options []elastic.ClientOptionFunc) error {
	client, err := elastic.NewClient(options...)
	if err != nil {
		return err
	}
	c.Client = client

	if c.Bulk.Name == "" {
		c.Bulk.Name = c.Name
	}

	if c.Bulk.Workers <= 0 {
		c.Bulk.Workers = 1
	}

	// 参数合理性校验
	if c.Bulk.RequestSize > 100*2024*1024 {
		c.log.Warn("Bulk RequestSize must be smaller than 100MB; it will be ignored.")
		c.Bulk.RequestSize = 100 * 2024 * 1024
	}
	if c.Bulk.ActionSize >= 10000 {
		c.log.Warn("Bulk ActionSize must be smaller than 10000; it will be ignored.")
		c.Bulk.ActionSize = 10000
	}
	if c.Bulk.FlushInterval >= 60 {
		c.log.Warn("Bulk FlushInterval must be smaller than 60s; it will be ignored.")
		c.Bulk.FlushInterval = time.Second * 60
	}
	if c.Bulk.AfterFunc == nil {
		// 默认回调：使用实例 logger 记录 bulk 执行错误
		clientLog := c.log
		c.Bulk.AfterFunc = func(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
			if err != nil || (response != nil && response.Errors) {
				res, _ := json.Marshal(response)
				clientLog.Error("bulk execution error",
					logx.Field("executionId", executionId),
					logx.Field("response", string(res)),
					logx.ErrField(err),
				)
			}
		}
	}
	if c.Bulk.Ctx == nil {
		c.Bulk.Ctx = context.Background()
	}

	c.BulkProcessor, err = c.Client.BulkProcessor().
		Name(c.Bulk.Name).
		Workers(c.Bulk.Workers).
		BulkSize(c.Bulk.ActionSize).
		BulkSize(c.Bulk.RequestSize).
		FlushInterval(c.Bulk.FlushInterval).
		Stats(true).
		After(c.Bulk.AfterFunc).
		Do(c.Bulk.Ctx)
	if err != nil {
		c.log.Error("init bulkProcessor error", logx.ErrField(err))
	}
	return nil
}

func defaultBulkFunc(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil || (response != nil && response.Errors) {
		res, _ := json.Marshal(response)
		logx.GetLogger().Error("bulk execution error",
			logx.Field("executionId", executionId),
			logx.Field("requests", requests),
			logx.Field("response", string(res)),
			logx.ErrField(err),
		)
	}
}

func DefaultBulk() *Bulk {
	return &Bulk{
		Workers:       3,
		FlushInterval: 1,
		ActionSize:    500,
		RequestSize:   5 << 20, // 5 MB,
		Ctx:           context.Background(),
	}
}

func CloseAll() {
	log := logx.GetLogger()
	for _, c := range clients {
		if c != nil {
			err := c.BulkProcessor.Close()
			if err != nil {
				log.Error("bulk close error", logx.ErrField(err))
			}
		}
	}
}

func (c *Client) AddIndexCache(indexName ...string) {
	for _, index := range indexName {
		c.CacheIndices.Store(index, true)
	}
}

func (c *Client) DeleteIndexCache(indexName ...string) {
	for _, index := range indexName {
		c.CacheIndices.Delete(index)
	}
}

func (c *Client) Close() error {
	return c.BulkProcessor.Close()
}

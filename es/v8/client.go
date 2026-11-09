package v8

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"

	"github.com/HeRedBo/pkg/logx"
)

var clients map[string]*Client

type Client struct {
	Name                     string
	Addr                     []string
	QueryLogEnable           bool
	GlobalSlowQueryMillisecond int64
	Username                 string
	password                 string
	BulkCfg                  *BulkCfg
	Client                   *elasticsearch.TypedClient
	BulkProcessor            esutil.BulkIndexer
	DebugMode                bool
	CacheIndices             sync.Map
	lock                     sync.Mutex
	log                      logx.Logger
	dslLog                   logx.Logger
}

type BulkCfg struct {
	Workers       int
	FlushInterval time.Duration
	ActionSize    int // 每批提交的文档数（注意：当前 esutil.BulkIndexerConfig 不支持此配置，仅作为记录保留）
	RequestSize   int // 每批提交的文档大小
}

// 定义常量
const (
	DefaultClient      = "es-default-client"
	DefaultReadClient  = "es-default-read-client"
	DefaultWriteClient = "es-default-write-client"
	SimpleClient       = "simple-es-client"
)

// option 连接配置项
type option struct {
	QueryLogEnable             bool
	GlobalSlowQueryMillisecond int64
	Bulk                       *BulkCfg
	DebugMode                  bool
	Scheme                     string
	logger                     logx.Logger // 业务日志
	dslLogger                  logx.Logger // DSL 查询日志（可与业务日志分离）
}

// Option 函数式选项
type Option func(*option)

// WithLogger 注入业务日志 Logger
func WithLogger(l logx.Logger) Option {
	return func(opt *option) {
		opt.logger = l
	}
}

// WithDSLLogger 注入 DSL 查询日志 Logger（可与业务日志分离）
func WithDSLLogger(l logx.Logger) Option {
	return func(opt *option) {
		opt.dslLogger = l
	}
}

// WithQueryLogEnable 开启查询日志
func WithQueryLogEnable(enable bool) Option {
	return func(opt *option) {
		opt.QueryLogEnable = enable
	}
}

// WithSlowQueryLogMillisecond 设置全局慢查询阈值（毫秒）
func WithSlowQueryLogMillisecond(ms int64) Option {
	return func(opt *option) {
		opt.GlobalSlowQueryMillisecond = ms
	}
}

// WithDebugMode 开启调试模式
func WithDebugMode(debug bool) Option {
	return func(opt *option) {
		opt.DebugMode = debug
	}
}

// WithScheme 设置连接 scheme（如 https）
func WithScheme(scheme string) Option {
	return func(opt *option) {
		opt.Scheme = scheme
	}
}

// WithBulk 设置 BulkIndexer 配置
func WithBulk(bulk *BulkCfg) Option {
	return func(opt *option) {
		opt.Bulk = bulk
	}
}

// DefaultBulkCfg 返回默认的 BulkCfg 配置
func DefaultBulkCfg() *BulkCfg {
	return &BulkCfg{
		Workers:       3,
		FlushInterval: 3 * time.Second,
		ActionSize:    500,
		RequestSize:   5 << 20, // 5 MB
	}
}

func InitClient(clientName string, addr []string, username, password string, opts ...Option) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}

	opt := &option{}
	for _, f := range opts {
		if f != nil {
			f(opt)
		}
	}

	logger := getLogger(opt.logger)
	dslLogger := getLogger(opt.dslLogger)

	bulkCfg := opt.Bulk
	if bulkCfg == nil {
		bulkCfg = DefaultBulkCfg()
	}

	client := &Client{
		Name:                       clientName,
		Addr:                       addr,
		QueryLogEnable:             opt.QueryLogEnable,
		GlobalSlowQueryMillisecond: opt.GlobalSlowQueryMillisecond,
		Username:                   username,
		password:                   password,
		BulkCfg:                    bulkCfg,
		DebugMode:                  opt.DebugMode,
		CacheIndices:               sync.Map{},
		lock:                       sync.Mutex{},
		log:                        logger,
		dslLog:                     dslLogger,
	}

	cfg := getBaseCfg(username, password, applyScheme(addr, opt.Scheme))
	esClient, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		return err
	}
	esBulkClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return err
	}

	bulkLog := logger
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        esBulkClient,
		NumWorkers:    bulkCfg.Workers,
		FlushBytes:    bulkCfg.RequestSize,
		FlushInterval: bulkCfg.FlushInterval,
		ErrorTrace:    true,
		OnError: func(ctx context.Context, err error) {
			if err != nil {
				bulkLog.Error("bulk index error", ErrField(err))
			}
		},
	})
	if err != nil {
		return err
	}

	client.BulkProcessor = bi
	client.Client = esClient
	clients[clientName] = client
	return nil
}

func getBaseCfg(username, password string, addr []string) elasticsearch.Config {
	cfg := elasticsearch.Config{
		Addresses: addr,
		Username:  username,
		Password:  password,
		Transport: &http.Transport{
			//DisableKeepAlives: true,
			Proxy: http.ProxyFromEnvironment,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				d := net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}
				return d.DialContext(ctx, network, addr)
			},
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			//针对es7.x+版本的https的密码连接，需要设置TLSClientConfig
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(i int) time.Duration {
			return time.Duration(i) * 100 * time.Millisecond
		},
		MaxRetries:    3,
		EnableMetrics: true,
	}

	return cfg
}

func GetDefaultClient() *http.Client {
	tr := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	}
	return &http.Client{Transport: tr}
}

func InitSimpleClient(addr []string, username, password string, opts ...Option) error {
	return InitClient(SimpleClient, addr, username, password, opts...)
}

func InitClientWithCfg(clientName string, cfg elasticsearch.Config, queryLogEnable bool, bulk BulkCfg, opts ...Option) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}

	opt := &option{}
	for _, f := range opts {
		if f != nil {
			f(opt)
		}
	}

	logger := getLogger(opt.logger)
	dslLogger := getLogger(opt.dslLogger)

	bulkCfg := &bulk
	if opt.Bulk != nil {
		bulkCfg = opt.Bulk
	}

	client := &Client{
		Name:                       clientName,
		Addr:                       cfg.Addresses,
		QueryLogEnable:             queryLogEnable,
		GlobalSlowQueryMillisecond: opt.GlobalSlowQueryMillisecond,
		Username:                   cfg.Username,
		password:                   cfg.Password,
		BulkCfg:                    bulkCfg,
		DebugMode:                  opt.DebugMode,
		CacheIndices:               sync.Map{},
		lock:                       sync.Mutex{},
		log:                        logger,
		dslLog:                     dslLogger,
	}

	if len(opt.Scheme) > 0 {
		cfg.Addresses = applyScheme(cfg.Addresses, opt.Scheme)
	}

	esClient, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		return err
	}
	esBulkClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return err
	}

	bulkLog := logger
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        esBulkClient,
		NumWorkers:    bulkCfg.Workers,
		FlushBytes:    bulkCfg.RequestSize,
		FlushInterval: bulkCfg.FlushInterval,
		ErrorTrace:    true,
		OnError: func(ctc context.Context, err error) {
			if err != nil {
				bulkLog.Error("bulk index error", ErrField(err))
			}
		},
	})
	if err != nil {
		return err
	}

	client.BulkProcessor = bi
	client.Client = esClient
	clients[clientName] = client
	return nil
}

func (c *Client) Close(ctx context.Context) error {
	return c.BulkProcessor.Close(ctx)
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

func GetSimpleClient() *Client {
	return GetClient(SimpleClient)
}

func CloseAll() {
	for _, c := range clients {
		if c != nil {
			err := c.BulkProcessor.Close(context.Background())
			if err != nil {
				c.log.Error("bulk close error", ErrField(err))
			}
		}
	}
}

func GetClient(name string) *Client {
	if client, exist := clients[name]; exist {
		return client
	}
	logx.GetLogger().Debug("call init before", Field("name", name))
	return nil
}

// applyScheme 为地址列表添加 scheme 前缀
func applyScheme(addrs []string, scheme string) []string {
	if len(scheme) == 0 {
		return addrs
	}
	result := make([]string, len(addrs))
	for i, addr := range addrs {
		if !strings.Contains(addr, "://") {
			result[i] = fmt.Sprintf("%s://%s", scheme, addr)
		} else {
			result[i] = addr
		}
	}
	return result
}

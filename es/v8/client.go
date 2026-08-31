package v8

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/olivere/elastic/v7"

	"github.com/HeRedBo/pkg/logx"
)

var clients map[string]*Client

type Client struct {
	Name           string
	Addr           []string
	QueryLogEnable bool
	Username       string
	password       string
	BulkCfg        *BulkCfg
	Client         *elasticsearch.TypedClient
	BulkProcessor  esutil.BulkIndexer
	DebugMode      bool
	CacheIndices   sync.Map
	lock           sync.Mutex
	log            logx.Logger
	dslLog         logx.Logger
}

type BulkCfg struct {
	Workers       int
	FlushInterval time.Duration
	ActionSize    int // 每批提交的文档数
	RequestSize   int // 每批提交的文档大小
	AfterFunc     elastic.BulkAfterFunc
	Ctx           context.Context
}

// 定义常量
const (
	DefaultClient      = "es-default-client"
	DefaultReadClient  = "es-default-read-client"
	DefaultWriteClient = "es-default-write-client"
)

// option 连接配置项
type option struct {
	logger    logx.Logger // 业务日志
	dslLogger logx.Logger // DSL 查询日志（可与业务日志分离）
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
	if dslLogger == logger {
		dslLogger = logger
	}

	client := &Client{
		Addr:           addr,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
		log:            logger,
		dslLog:         dslLogger,
	}
	cfg := getBaseCfg(username, password, addr)
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
		FlushInterval: 3 * time.Second,
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
	if dslLogger == logger {
		dslLogger = logger
	}

	client := &Client{
		Addr:           cfg.Addresses,
		QueryLogEnable: false,
		Username:       cfg.Username,
		password:       cfg.Password,
		BulkCfg:        &bulk,
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
		log:            logger,
		dslLog:         dslLogger,
	}
	client.QueryLogEnable = queryLogEnable
	client.BulkCfg = &bulk

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
		NumWorkers:    bulk.Workers,
		FlushBytes:    bulk.RequestSize,
		FlushInterval: bulk.FlushInterval,
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

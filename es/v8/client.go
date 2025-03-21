package v8

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/olivere/elastic/v7"
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

func InitClient(clientName string, addr []string, username, password string) error {

	if clients == nil {
		clients = make(map[string]*Client, 0)
	}

	client := &Client{
		Addr:           addr,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
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

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        esBulkClient,
		FlushInterval: 3 * time.Second,
		ErrorTrace:    true,
		OnError: func(ctx context.Context, err error) {
			if err != nil {
				log.Printf("Bulk error : %+v", err)
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

func getDefaultClient() *http.Client {
	tr := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	}
	return &http.Client{Transport: tr}
}

func InitClientWithCfg(clientName string, cfg elasticsearch.Config, queryLogEnable bool, bulk BulkCfg) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}

	client := &Client{
		Addr:           cfg.Addresses,
		QueryLogEnable: false,
		Username:       cfg.Username,
		password:       cfg.Password,
		BulkCfg:        &bulk,
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
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

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        esBulkClient,
		NumWorkers:    bulk.Workers,
		FlushBytes:    bulk.RequestSizse,
		FlushInterval: bulk.FlushInterval,
		ErrorTrace:    true,
		OnError: func(ctc context.Context, err error) {
			if err != nil {
				log.Printf("Bulk error : %+v", err)
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
				log.Print("bulk close error", err)
			}
		}
	}
}

func GetClient(name string) *Client {
	if client, exist := clients[name]; exist {
		return client
	}
	log.Print("call init", name, "before !!!")
	return nil
}

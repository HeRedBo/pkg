package cache

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"time"
)

var redisClients = make(map[string]*Redis)

type Redis struct {
	client        *redis.Client
	clusterClient *redis.ClusterClient
}

const (
	DefaultRedisClient = "default-redis-client"
	MinIdleConn        = 50
	PoolSize           = 20
	MaxRetries         = 3
)

func setDefaultOptions(opt *redis.Options) {
	if opt.DialTimeout == 0 {
		opt.DialTimeout = 2 * time.Second
	}
	if opt.ReadTimeout == 0 {
		//默认值为3秒
		opt.ReadTimeout = 2 * time.Second
	}
	if opt.PoolTimeout == 0 {
		//默认为ReadTimeout + 1秒（4s）
		opt.PoolTimeout = 10 * time.Second
	}
}

func setDefaultClusterOptions(opt *redis.ClusterOptions) {
	if opt.DialTimeout == 0 {
		opt.DialTimeout = 2 * time.Second
	}

	if opt.ReadTimeout == 0 {
		//默认值为3秒
		opt.ReadTimeout = 2 * time.Second
	}

	if opt.ReadTimeout == 0 {
		//默认值与ReadTimeout相等
		opt.ReadTimeout = 2 * time.Second
	}
	if opt.PoolTimeout == 0 {
		//默认为ReadTimeout + 1秒（4s）
		opt.PoolTimeout = 10 * time.Second
	}

}

func initRedis(clientName string, opt *redis.Options) error {
	if len(clientName) == 0 {
		return errors.New("empty client name")
	}

	if len(opt.Addr) == 0 {
		return errors.New("empty addr")
	}

	setDefaultOptions(opt)

	client := redis.NewClient(opt)
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return err
	}

	redisClients[clientName] = &Redis{
		client: client,
	}
	return nil
}

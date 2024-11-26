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

var Ctx = context.Background()

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
	if err := client.Ping(Ctx).Err(); err != nil {
		return err
	}

	redisClients[clientName] = &Redis{
		client: client,
	}
	return nil
}

func InitClusterRedis(clientName string, opt *redis.ClusterOptions) error {
	if len(clientName) == 0 {
		return errors.New("empty client name")
	}

	if len(opt.Addrs) == 0 {
		return errors.New("empty addr")
	}

	setDefaultClusterOptions(opt)

	client := redis.NewClusterClient(opt)
	if err := client.Ping(Ctx).Err(); err != nil {
		return err
	}

	redisClients[clientName] = &Redis{
		clusterClient: client,
	}
	return nil
}

func GetRedisClient(name string) *Redis {
	if client, ok := redisClients[name]; ok {
		return client
	}
	return nil
}

func GetRedisClusterClient(name string) *Redis {
	if client, ok := redisClients[name]; ok {
		return client
	}
	return nil
}

func (r *Redis) Set(key string, value interface{}, ttl time.Duration) error {
	if len(key) == 0 {
		return errors.New("emtpy key")
	}
	if r.client != nil {
		if err := r.client.Set(Ctx, key, value, ttl).Err(); err != nil {
			// TODO 返回错误信息优化
			return err
		}
		return nil
	}

	// 集群版
	if err := r.clusterClient.Set(Ctx, key, value, ttl).Err(); err != nil {
		return err
	}
	return nil
}

func (r *Redis) Get(key string) interface{} {
	if len(key) == 0 {
		CacheStdLogger.Println("empty key")
		return nil
	}

	if r.client != nil {
		value, err := r.client.Get(Ctx, key).Result()
		if err != nil && err != redis.Nil {
			CacheStdLogger.Printf("redis get key: %s err %v", key, err)
		}
		return value
	}

	value, err := r.clusterClient.Get(Ctx, key).Result()
	if err != nil && err != redis.Nil {
		CacheStdLogger.Printf("redis get key: %s err %v", key, err)
	}
	return value
}

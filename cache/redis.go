package cache

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"strings"
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

func (r *Redis) GetStr(key string) (value string, err error) {
	if len(key) == 0 {
		err = errors.New("emtpy key")
		return
	}
	if r.client != nil {
		value, err = r.client.Get(Ctx, key).Result()
		if err != nil && err != redis.Nil {
			return "", err
		}
	}

	value, err = r.clusterClient.Get(Ctx, key).Result()
	if err != nil && err != redis.Nil {
		return "", err
	}
	return
}

// TTL ttl get some  key from redis
func (r *Redis) TTL(key string) (time.Duration, error) {
	if len(key) == 0 {
		return 0, errors.New("empty key")
	}
	if r.client != nil {
		ttl, err := r.client.TTL(Ctx, key).Result()
		if err != nil && err != redis.Nil {
			return -1, err
		}
		return ttl, nil
	}
	ttl, err := r.clusterClient.TTL(Ctx, key).Result()
	if err != nil && err != redis.Nil {
		return -1, err
	}
	return ttl, nil
}

// Expire expire some key
func (r *Redis) Expire(key string, ttl time.Duration) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("empty key")
	}
	if r.client != nil {
		ok, err := r.client.Expire(Ctx, key, ttl).Result()
		return ok, err
	}
	ok, err := r.clusterClient.Expire(Ctx, key, ttl).Result()
	return ok, err
}

// ExpireAt expire some key at some time
func (r *Redis) ExpireAt(key string, ttl time.Time) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("empty key")
	}
	if r.client != nil {
		ok, err := r.client.ExpireAt(Ctx, key, ttl).Result()
		return ok, err
	}
	ok, err := r.clusterClient.ExpireAt(Ctx, key, ttl).Result()
	return ok, err
}

// Delete delete redis key
func (r *Redis) Delete(key string) error {
	if len(key) == 0 {
		return errors.New("empty keys")
	}
	if r.client != nil {
		_, err := r.client.Del(Ctx, key).Result()
		return err
	}
	_, err := r.clusterClient.Del(Ctx, key).Result()
	return err
}

func (r *Redis) IsExist(key string) bool {
	if len(key) == 0 {
		return false
	}
	if r.client != nil {
		value, err := r.client.Exists(Ctx, key).Result()
		if err != nil && err != redis.Nil {
			CacheStdLogger.Printf("cmd : Exists ; key : %s ; err : %v", key, err)
		}
		return value > 0
	}
	value, err := r.clusterClient.Exists(Ctx, key).Result()
	if err != nil && err != redis.Nil {
		CacheStdLogger.Printf("cmd : Exists ; key : %s ; err : %v", key, err)
	}
	return value > 0
}

func (r *Redis) Exists(keys ...string) (bool, error) {
	if len(keys) == 0 {
		return false, errors.New("empty keys")
	}

	if r.client != nil {
		value, err := r.client.Exists(Ctx, keys...).Result()
		return value > 0, err
	}
	value, err := r.clusterClient.Exists(Ctx, keys...).Result()
	return value > 0, err
}

func (r *Redis) Incr(key string) (value int64, err error) {
	if len(key) == 0 {
		return 0, errors.New("empty key")
	}
	if r.client != nil {
		value, err = r.client.Incr(Ctx, key).Result()
		return
	}
	value, err = r.clusterClient.Incr(Ctx, key).Result()
	return
}

// Close redis 关闭
func (r *Redis) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return r.clusterClient.Close()
}

// Version 获取Redis 版本信息
func (r *Redis) Version() string {
	if r.client != nil {
		server := r.client.Info(Ctx, "server").Val()
		spl1 := strings.Split(server, "# Server")
		spl2 := strings.Split(spl1[1], "redis_version:")
		spl3 := strings.Split(spl2[1], "redis_git_sha1:")
		return spl3[0]
	}
	server := r.clusterClient.Info(Ctx, "server").Val()
	spl1 := strings.Split(server, "# Server")
	spl2 := strings.Split(spl1[1], "redis_version:")
	spl3 := strings.Split(spl2[1], "redis_git_sha1:")
	return spl3[0]
}

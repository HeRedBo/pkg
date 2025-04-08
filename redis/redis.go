package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strings"
	"time"
)

// RedisConfig 定义 Redis 配置
type Config struct {
	// 单机模式地址
	Addr string
	// 集群模式地址列表
	Addrs []string // 通过判断 集群模式地址是否为空 判断是否是集群模式
	// 密码
	Password string
	// 数据库
	DB int
}

func (c *Config) IsCluste() bool {
	return len(c.Addrs) > 0
}

// Client 定义 Redis 客户端结构体
type Client struct {
	client redis.UniversalClient
}

// NewRedisClient 创建 Redis 客户端实例
func NewRedisClient(config Config) (*Client, error) {
	var client redis.UniversalClient
	if config.IsCluste() {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    config.Addrs,
			Password: config.Password,
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr:     config.Addr,
			Password: config.Password,
			DB:       config.DB,
		})
	}
	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Client{
		client: client,
	}, nil
}

func (r *Client) GetClient() redis.UniversalClient {
	return r.client
}

// Set 设置键值对
func (r *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

// Get 获取键对应的值
func (r *Client) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

// Del 删除键
func (r *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	return r.client.Del(ctx, keys...).Result()
}

func (r *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	if len(key) == 0 {
		return 0, errors.New("empty key")
	}
	ttl, err := r.client.TTL(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return -1, err
	}
	return ttl, nil
}
func (r *Client) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("empty key")
	}
	ok, err := r.client.Expire(ctx, key, ttl).Result()
	return ok, err
}

func (r *Client) ExpireAt(ctx context.Context, key string, ttl time.Time) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("empty key")
	}
	return r.client.ExpireAt(ctx, key, ttl).Result()
}

func (r *Client) Exists(ctx context.Context, keys ...string) (bool, error) {
	if len(keys) == 0 {
		return false, errors.New("empty key")
	}
	value, err := r.client.Exists(ctx, keys...).Result()
	return value > 0, err
}

func (r *Client) IsExist(ctx context.Context, key string) bool {
	if len(key) == 0 {
		return false
	}
	value, err := r.client.Exists(ctx, key).Result()
	if err != nil && err != redis.Nil {
		// TODO 记录日志
		fmt.Println(err)
	}
	return value > 0
}

func (r *Client) Delete(ctx context.Context, key string) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}
	_, err := r.client.Del(ctx, key).Result()
	return err
}

func (r *Client) Incr(ctx context.Context, key string) (value int64, err error) {
	if len(key) == 0 {
		return 0, errors.New("empty key")
	}
	value, err = r.client.Incr(ctx, key).Result()
	return
}

// Close 关闭 Redis 客户端连接
func (r *Client) Close() error {
	return r.client.Close()
}

func (r *Client) Version(ctx context.Context) string {
	server := r.client.Info(ctx).Val()
	spl1 := strings.Split(server, "# Server")
	spl2 := strings.Split(spl1[1], "redis_version:")
	spl3 := strings.Split(spl2[1], "redis_git_sha1:")
	//dump.Println(server)
	return spl3[0]
}

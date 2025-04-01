package redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

// RedisConfig 定义 Redis 配置

type Config struct {
	// 单机模式地址
	Addr string
	// 集群模式地址列表
	Addrs []string
	// 密码
	Password string
	// 数据库
	DB int
	// 是否为集群模式
	IsCluster bool
}

// RedisClient 定义 Redis 客户端结构体
type Client struct {
	client redis.UniversalClient
}

// NewRedisClient 创建 Redis 客户端实例
func NewRedisClient(config Config) (*Client, error) {
	var client redis.UniversalClient
	if config.IsCluster {
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

// Close 关闭 Redis 客户端连接
func (r *Client) Close() error {
	return r.client.Close()
}

package cache

import (
	"time"

	"github.com/go-redis/redis/v7"

	"github.com/HeRedBo/pkg/logx"
)

/**
 * 定义缓存接口
 */
type Cache interface {
	Set(key string, value interface{}, ttl time.Duration) error
	Get(key string) interface{}
	GetStr(key string) (value string, err error)
	TTL(key string) (time.Duration, error)
	Expire(key string, ttl time.Duration) (bool, error)
	Delete(key string) error
	Exists(key ...string) (bool, error)
	IsExist(key string) bool
	Incr(key string) (int64, error)
	SetBit(key string, offset int64, val int) (value int64, err error)
	GetBit(key string, offset int64) (value int64, err error)
	SetBigBit(key string, offset int64, val int) (value int64, err error)
	GetBigBit(key string, offset int64) (value int64, err error)
	SetBitNOBucket(key string, offset int64, val int) (value int64, err error)
	GetBitNOBucket(key string, offset int64) (value int64, err error)
	BitCountNOBucket(key string, start, end int64) (value int64, err error)
	Close() error
	Version() string
}

// option 缓存连接配置项
type option struct {
	logger logx.Logger
}

// Option 函数式选项
type Option func(*option)

// WithLogger 通过 Option 注入 Logger（优先级最高）
func WithLogger(l logx.Logger) Option {
	return func(opt *option) {
		opt.logger = l
	}
}

// Redis 缓存实现
type Redis struct {
	client        *redis.Client
	clusterClient *redis.ClusterClient
	log           logx.Logger
}

package limiter

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type LimiterType int

const (
	TokenBucket   LimiterType = iota // 令牌桶
	LeakyBucket                      // 漏桶
	FixedWindow                      // 固定窗口
	SlidingWindow                    // 滑动窗口
)

type Config struct {
	Key       string        // Redis Key 前缀（单机不用）
	Rate      int           // 每秒速率
	Burst     int           // 桶容量
	Expire    time.Duration // Redis 过期时间
	Window    time.Duration // 窗口大小
	WindowEnd time.Time     // 当前窗口结束时间
}

// Limiter
// ###########################
// 1. 顶层通用接口（业务只认它）
// ###########################
type Limiter interface {
	Allow() (bool, error)
}

// 构造函数定义
type creator func(cfg Config, cli redis.Cmdable) Limiter

// 全局注册中心
var (
	localRegistry = make(map[LimiterType]creator)
	redisRegistry = make(map[LimiterType]creator)
)

// 提供给 local 包注册
func RegisterLocal(typ LimiterType, c creator) {
	localRegistry[typ] = c
}

// 提供给 redis 包注册
func RegisterRedis(typ LimiterType, c creator) {
	redisRegistry[typ] = c
}

// NewLimiter 自动判断：
// 传入 redis → 分布式限流器
// 传 nil → 单机限流器

// 超级工厂：自动选择 算法 + 单机/Redis
func NewLimiter(typ LimiterType, cfg Config, cli redis.Cmdable) Limiter {
	if cli != nil {
		fn, ok := redisRegistry[typ]
		if !ok {
			panic("redis limiter not registered")
		}
		return fn(cfg, cli)
	}

	fn, ok := localRegistry[typ]
	if !ok {
		panic("local limiter not registered")
	}
	return fn(cfg, nil)
}

package redis

import (
	"context"
	"time"

	"github.com/HeRedBo/pkg/limiter"
	"github.com/redis/go-redis/v9"
)

// LeakyBucket 分布式漏桶
type LeakyBucket struct {
	client redis.Cmdable
	cfg    limiter.Config
	script *redis.Script
}

func init() {
	limiter.RegisterRedis(limiter.LeakyBucket, func(cfg limiter.Config, client redis.Cmdable) limiter.Limiter {
		return NewLeakyBucket(client, cfg)
	})
}

func NewLeakyBucket(client redis.Cmdable, cfg limiter.Config) *LeakyBucket {
	script := redis.NewScript(`
		local k = KEYS[1]
		local capacity = tonumber(ARGV[1])
		local now = tonumber(ARGV[2])
		local exp = tonumber(ARGV[3])

		local water = tonumber(redis.call("HGET", k, "water")) or 0
		if water < capacity then
			water = water + 1
			redis.call("HMSET", k, "water", water, "last", now)
			redis.call("EXPIRE", k, exp)
			return 1
		end
		return 0
	`)

	return &LeakyBucket{
		client: client,
		cfg:    cfg,
		script: script,
	}
}

func (r *LeakyBucket) Allow() (bool, error) {
	ctx := context.Background()
	now := time.Now().UnixMilli()

	resp, err := r.script.Run(ctx, r.client,
		[]string{r.cfg.Key},
		r.cfg.Burst,
		r.cfg.Rate,
		now,
		int64(r.cfg.Expire.Seconds()),
	).Result()

	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

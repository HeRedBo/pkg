package redis

import (
	"context"
	"time"

	"github.com/HeRedBo/pkg/limiter"
	"github.com/redis/go-redis/v9"
)

type TokenBucket struct {
	client redis.Cmdable
	cfg    limiter.Config
	script *redis.Script
}

func init() {
	limiter.RegisterRedis(limiter.TokenBucket, func(cfg limiter.Config, client redis.Cmdable) limiter.Limiter {
		return NewTokenBucket(client, cfg)
	})
}

func NewTokenBucket(client redis.Cmdable, cfg limiter.Config) *TokenBucket {
	script := redis.NewScript(`
		local key = KEYS[1]
		local rate = tonumber(ARGV[1])
		local burst = tonumber(ARGV[2])
		local now = tonumber(ARGV[3])
		local expire = tonumber(ARGV[4])

		local data = redis.call("HMGET", key, "tokens", "last")
		local tokens = tonumber(data[1])
		local last = tonumber(data[2])

		if not tokens or not last then
			tokens = burst
			last = now
		end

		local elapsed = math.max(0, (now - last)/1000)
		tokens = tokens + elapsed * rate
		if tokens > burst then
			tokens = burst
		end

		local ok = 0
		if tokens >= 1 then
			tokens = tokens - 1
			ok = 1
		end

		redis.call("HMSET", key, "tokens", tokens, "last", now)
		redis.call("EXPIRE", key, expire)
		return ok
	`)

	return &TokenBucket{
		client: client,
		cfg:    cfg,
		script: script,
	}
}

func (r *TokenBucket) Allow() (bool, error) {
	ctx := context.Background()
	now := time.Now().UnixMilli()

	resp, err := r.script.Run(ctx, r.client,
		[]string{r.cfg.Key},
		r.cfg.Rate,
		r.cfg.Burst,
		now,
		int64(r.cfg.Expire.Seconds()),
	).Result()

	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

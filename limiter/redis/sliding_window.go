package redis

import (
	"context"
	"time"

	"github.com/HeRedBo/pkg/limiter"
	"github.com/redis/go-redis/v9"
)

type SlidingWindow struct {
	client redis.Cmdable
	cfg    limiter.Config
	script *redis.Script
}

func init() {
	limiter.RegisterRedis(limiter.SlidingWindow, func(cfg limiter.Config, client redis.Cmdable) limiter.Limiter {
		return NewSlidingWindow(client, cfg)
	})
}

func NewSlidingWindow(client redis.Cmdable, cfg limiter.Config) *SlidingWindow {
	script := redis.NewScript(`
		local key = KEYS[1]
		local limit = tonumber(ARGV[1])
		local window_ms = tonumber(ARGV[2])  -- 改回毫秒
		local now = tonumber(ARGV[3])
		local expire = tonumber(ARGV[4])

		-- 清理过期数据
		redis.call("ZREMRANGEBYSCORE", key, 0, now - window_ms)

		-- 统计数量
		local count = redis.call("ZCARD", key)
		if count >= limit then
			return 0
		end

		-- 每个请求用唯一值，确保同一毫秒也能计数
		redis.call("ZADD", key, now, tostring(now) .. redis.call("INCR", key..":seq"))
		redis.call("EXPIRE", key, expire)
		redis.call("EXPIRE", key..":seq", expire)
		return 1
	`)

	return &SlidingWindow{
		client: client,
		cfg:    cfg,
		script: script,
	}
}

func (r *SlidingWindow) Allow() (bool, error) {
	ctx := context.Background()
	now := time.Now().UnixMilli()

	resp, err := r.script.Run(ctx, r.client,
		[]string{r.cfg.Key},
		r.cfg.Rate,
		1000,
		now,
		int64(r.cfg.Expire.Seconds()), // 过期时间必须传！
	).Result()

	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

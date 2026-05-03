package redis

import (
	"context"
	"time"

	"github.com/HeRedBo/pkg/limiter"
	"github.com/redis/go-redis/v9"
)

type FixedWindow struct {
	client redis.Cmdable
	cfg    limiter.Config
	script *redis.Script
}

func init() {
	limiter.RegisterRedis(limiter.FixedWindow, func(cfg limiter.Config, client redis.Cmdable) limiter.Limiter {
		return NewFixedWindow(client, cfg)
	})
}

func NewFixedWindow(client redis.Cmdable, cfg limiter.Config) *FixedWindow {
	script := redis.NewScript(`
		local key = KEYS[1]
		local limit = tonumber(ARGV[1])
		local window = tonumber(ARGV[2])
		local now = tonumber(ARGV[3])

		local count = redis.call("GET", key)
		if not count then
			redis.call("SET", key, 1, "EX", window)
			return 1
		end

		count = tonumber(count)
		if count < limit then
			redis.call("INCR", key)
			return 1
		end
		return 0
	`)

	return &FixedWindow{
		client: client,
		cfg:    cfg,
		script: script,
	}
}

func (r *FixedWindow) Allow() (bool, error) {
	ctx := context.Background()
	now := time.Now().Unix()
	resp, err := r.script.Run(ctx, r.client,
		[]string{r.cfg.Key},
		r.cfg.Rate,
		int64(r.cfg.Window.Seconds()),
		now,
	).Result()

	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

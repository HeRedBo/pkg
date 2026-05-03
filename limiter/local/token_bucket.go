package local

import (
	"sync"
	"time"

	"github.com/HeRedBo/pkg/limiter"
	"github.com/redis/go-redis/v9"
)

type TokenBucket struct {
	mu       sync.Mutex
	rate     float64
	burst    float64
	tokens   float64
	lastTime time.Time
}

func init() {
	limiter.RegisterLocal(limiter.TokenBucket, func(cfg limiter.Config, _ redis.Cmdable) limiter.Limiter {
		return NewTokenBucket(cfg)
	})
}

func NewTokenBucket(cfg limiter.Config) *TokenBucket {
	return &TokenBucket{
		rate:     float64(cfg.Rate),
		burst:    float64(cfg.Burst),
		tokens:   float64(cfg.Burst),
		lastTime: time.Now(),
	}
}

func (l *TokenBucket) Allow() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastTime).Seconds()
	l.tokens += elapsed * l.rate

	if l.tokens > l.burst {
		l.tokens = l.burst
	}

	if l.tokens >= 1 {
		l.tokens--
		l.lastTime = now
		return true, nil
	}
	return false, nil
}

package local

import (
	"sync"
	"time"

	"github.com/HeRedBo/pkg/limiter"
	"github.com/redis/go-redis/v9"
)

type FixedWindow struct {
	mu        sync.Mutex
	rate      int
	window    time.Duration
	count     int
	windowEnd time.Time
}

func init() {
	limiter.RegisterLocal(limiter.FixedWindow, func(cfg limiter.Config, _ redis.Cmdable) limiter.Limiter {
		return NewFixedWindow(cfg)
	})
}

func NewFixedWindow(cfg limiter.Config) *FixedWindow {
	return &FixedWindow{
		rate:      cfg.Rate,
		window:    cfg.Window,
		windowEnd: time.Now().Add(cfg.Window),
	}
}

func (l *FixedWindow) Allow() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if now.After(l.windowEnd) {
		l.count = 0
		l.windowEnd = now.Add(l.window)
	}

	if l.count < l.rate {
		l.count++
		return true, nil
	}
	return false, nil
}

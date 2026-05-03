package local

import (
	"sync"
	"time"

	"github.com/HeRedBo/pkg/limiter"
	"github.com/redis/go-redis/v9"
)

type SlidingWindow struct {
	mu         sync.Mutex
	rate       int
	window     time.Duration
	timestamps []time.Time
}

func init() {
	limiter.RegisterLocal(limiter.SlidingWindow, func(cfg limiter.Config, _ redis.Cmdable) limiter.Limiter {
		return NewSlidingWindow(cfg)
	})
}

func NewSlidingWindow(cfg limiter.Config) *SlidingWindow {
	return &SlidingWindow{
		rate:   cfg.Rate,
		window: cfg.Window,
	}
}

func (l *SlidingWindow) Allow() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	border := now.Add(-l.window)

	idx := 0
	for i, t := range l.timestamps {
		if t.After(border) {
			idx = i
			break
		}
	}
	l.timestamps = l.timestamps[idx:]

	if len(l.timestamps) < l.rate {
		l.timestamps = append(l.timestamps, now)
		return true, nil
	}
	return false, nil
}

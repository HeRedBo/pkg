package limiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/HeRedBo/pkg/limiter"
	// 必须匿名导入，让 init() 注册算法
	"github.com/HeRedBo/pkg/limiter/local"
	_ "github.com/HeRedBo/pkg/limiter/redis"
	"github.com/redis/go-redis/v9"
)

var testCfg = limiter.Config{
	Rate:   5,
	Burst:  5,
	Window: time.Second,
	Expire: 10 * time.Second,
	Key:    "test:limiter",
}

// 获取Redis客户端
func getRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "123456",
	})
}

// 测试本地所有限流器
func TestLocalAll(t *testing.T) {
	// 手动创建实例（不走工厂，避免注册问题）
	lb := local.NewTokenBucket(testCfg)
	testLimiter(t, lb)

	llb := local.NewLeakyBucket(testCfg)
	testLimiter(t, llb)

	lfw := local.NewFixedWindow(testCfg)
	testLimiter(t, lfw)

	lsw := local.NewSlidingWindow(testCfg)
	testLimiter(t, lsw)
}

// ----------------------------------------------------------------
// 测试所有 单机（local）实现 → 你写的这种，完全可以！
// ----------------------------------------------------------------
func TestLocal_Limiter(t *testing.T) {
	tests := []struct {
		name string
		typ  limiter.LimiterType
	}{
		{"token_bucket", limiter.TokenBucket},
		{"leaky_bucket", limiter.LeakyBucket},
		{"fixed_window", limiter.FixedWindow},
		{"sliding_window", limiter.SlidingWindow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := limiter.NewLimiter(tt.typ, testCfg, nil)

			// 前 5 个通过
			for i := 0; i < 5; i++ {
				ok, err := l.Allow()
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected allow")
				}
			}

			// 第 6 个限流
			ok, _ := l.Allow()
			if ok {
				t.Fatal("expected limit")
			}
		})
	}
}

// 通用测试逻辑：前5个通过，第6个限流
func testLimiter(t *testing.T, l limiter.Limiter) {
	t.Helper()

	// 前5次通过
	for i := 0; i < 5; i++ {
		ok, err := l.Allow()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected allow")
		}
	}

	// 第6次限流
	ok, _ := l.Allow()
	if ok {
		t.Fatal("expected limit")
	}
}

// ----------------------------------------------------------------
// 测试所有 Redis 实现
// ----------------------------------------------------------------
func TestRedis_Limiter(t *testing.T) {
	rdb := getRedisClient()
	defer rdb.Close()

	rdb.Del(context.Background(), testCfg.Key)

	tests := []struct {
		name string
		typ  limiter.LimiterType
	}{
		{"token_bucket", limiter.TokenBucket},
		{"leaky_bucket", limiter.LeakyBucket},
		{"fixed_window", limiter.FixedWindow},
		{"sliding_window", limiter.SlidingWindow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 每个算法独立 KEY，绝对不互相污染
			cfg := testCfg
			cfg.Key = "test:limiter:" + tt.name // 每个子测试独立 key
			// 🔥 关键修复：每次都清空数据
			rdb.Del(context.Background(), cfg.Key)
			l := limiter.NewLimiter(tt.typ, cfg, rdb)

			for i := 0; i < 5; i++ {
				ok, err := l.Allow()
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					t.Fatal("expected allow")
				}

			}

			ok, _ := l.Allow()
			if ok {
				t.Fatal("expected limit")
			}
		})
	}
}

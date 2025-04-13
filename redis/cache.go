package redis

import (
	"context"
	"log"
	"os"
	"time"
)

type Cache interface {
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Get(ctx context.Context, key string) interface{}
	GetStr(ctx context.Context, key string) (value string, err error)
	TTL(ctx context.Context, key string) (time.Duration, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	ExpireAt(ctx context.Context, key string, ttl time.Time) (bool, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, keys ...string) (bool, error)
	IsExist(ctx context.Context, key string) bool
	Incr(ctx context.Context, key string) (int64, error)
	SetBit(ctx context.Context, key string, offset int64, val int) (value int64, err error)
	GetBit(ctx context.Context, key string, offset int64) (value int64, err error)
	SetBigBit(ctx context.Context, key string, offset int64, val int) (value int64, err error)
	GetBigBit(ctx context.Context, key string, offset int64) (value int64, err error)
	SetBitNOBucket(ctx context.Context, key string, offset int64, val int) (value int64, err error)
	GetBitNOBucket(ctx context.Context, key string, offset int64) (value int64, err error)
	BitCountNOBucket(ctx context.Context, key string, start, end int64) (value int64, err error)
	Close() error
	Version(ctx context.Context) string
}

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var CacheStdLogger stdLogger

func init() {
	CacheStdLogger = log.New(os.Stdout, "[Cache] ", log.LstdFlags|log.Lshortfile)
}

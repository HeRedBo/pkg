package redis

import (
	"context"
	"fmt"
	"github.com/HeRedBo/pkg/compression"
	"github.com/gookit/goutil/dump"
	"testing"
	"time"
)

type UserTest struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func TestGet(t *testing.T) {
	key := "test"

	user := UserTest{
		ID:   1,
		Name: "RedBo",
	}
	userByte, err := compression.MarshalJsonAndGzip(user)
	if err != nil {
		t.Errorf("MarshalJsonAndGzip err %v", err)
	}

	singleConfig := Config{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}
	client, err := NewRedisClient(singleConfig)
	defer client.Close()
	if err != nil {
		fmt.Println("Failed to create Redis client:", err)
		return
	}
	ctx := context.Background()
	expiration := 10 * time.Second
	// 设置键值对
	err = client.Set(ctx, key, userByte, expiration)
	if err != nil {
		t.Errorf("Failed to set key err %v", err)
	}
	val, err := client.Get(ctx, key)
	output := UserTest{}
	err = compression.UnmarshalDataFromJsonWithGzip([]byte(val), &output)
	if err != nil {
		t.Error("UnmarshalDataFromJsonWithGzip error", val, err)
	}
	dump.Println(output)
	t.Log(output)
}

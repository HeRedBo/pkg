package main

import (
	"context"
	"fmt"
	"pkg/redis"
	"time"
)

func main() {
	// 单机模式配置示例
	singleConfig := redis.Config{
		Addr:      "localhost:6379",
		Password:  "",
		DB:        0,
		IsCluster: false,
	}

	// 集群模式配置示例
	//clusterConfig := redis.Config{
	//	Addrs:     []string{"node1:6379", "node2:6379", "node3:6379"},
	//	Password:  "",
	//	IsCluster: true,
	//}

	// 创建 Redis 客户端实例
	client, err := redis.NewRedisClient(singleConfig)
	if err != nil {
		fmt.Println("Failed to create Redis client:", err)
		return
	}
	defer client.Close()

	ctx := context.Background()
	key := "test_key"
	value := "test_value"
	expiration := 10 * time.Second

	// 设置键值对
	err = client.Set(ctx, key, value, expiration)
	if err != nil {
		fmt.Println("Failed to set key:", err)
		return
	}

	// 获取键对应的值
	result, err := client.Get(ctx, key)
	if err != nil {
		fmt.Println("Failed to get key:", err)
		return
	}
	fmt.Println("Value:", result)
	// 删除键
	deleted, err := client.Del(ctx, key)
	if err != nil {
		fmt.Println("Failed to delete key:", err)
		return
	}
	fmt.Println("Deleted keys:", deleted)
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/HeRedBo/pkg/redis"
	"github.com/gookit/goutil/dump"
	"log"
	"time"
)

type UserTest struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func main() {
	// 单机模式配置示例
	singleConfig := redis.Config{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}
	// 集群模式配置示例
	//clusterConfig := redis.Config{
	//	Addrs:     []string{"node1:6379", "node2:6379", "node3:6379"},
	//	Password:  "",
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
	//
	//// 获取键对应的值
	//result, err := client.Get(ctx, key)
	//if err != nil {
	//	fmt.Println("Failed to get key:", err)
	//	return
	//}
	//fmt.Println("Value:", result)
	//// 获取 过期时间
	//ttl, err := client.GetClient().TTL(ctx, key).Result()
	//fmt.Println("TTL:", ttl)

	//version := client.Version(ctx)
	//dump.Println(version)

	// 删除键
	//deleted, err := client.Del(ctx, key)
	//if err != nil {
	//	fmt.Println("Failed to delete key:", err)
	//	return
	//}
	//fmt.Println("Deleted keys:", deleted)

	// // 创建用户实例
	user := UserTest{
		ID:   1,
		Name: "Redbo",
	}
	// 序列化 json
	userJson, err := json.Marshal(user)
	if err != nil {
		dump.Println(err)
		return
	}
	user_key := "user_key:1"
	time_expiration := 10 * time.Second
	// 存储到 Redis（带过期时间）
	err = client.Set(ctx, user_key, userJson, time_expiration)
	if err != nil {
		fmt.Println("Failed to set user_key:", err)
		return
	}
	// 从 Redis 读取数据
	val, err := client.Get(ctx, user_key)
	if err != nil {
		log.Fatal("Redis 读取失败:", err)
	}
	// 反序列化为结构体
	var retrievedUser UserTest
	err = json.Unmarshal([]byte(val), &retrievedUser)
	if err != nil {
		log.Fatal("JSON 反序列化失败:", err)
	}
	dump.Println(retrievedUser)

	//useByte, err := compression

}

package main

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"pkg/kafka"
	"sync"
	"syscall"
)

// ================== 使用示例 ==================

func main1() {
	// 生产者示例
	producer, err := kafka.NewKafkaProducer(kafka.ProducerConfig{
		Brokers: []string{"host.docker.internal:9092"},
		Topic:   "test-topic",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	err = producer.SendMessage("", []byte("key"), []byte("Hello Kafka!"))
	if err != nil {
		log.Printf("发送消息失败: %v", err)
	}

	// 消费者示例
	consumer, err := kafka.NewKafkaConsumer(kafka.ConsumerConfig{
		Brokers:    []string{"host.docker.internal:9092"},
		GroupID:    "test-group",
		Topics:     []string{"test-topic"},
		AutoOffset: "oldest", // 改为 oldest 确保能消费历史消息
		ShowLogs:   true,
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Start(ctx, func(msg *sarama.ConsumerMessage) error {
			log.Printf("收到消息: 主题=%s, 分区=%d, 偏移=%d, 键=%s, 值=%s",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			return nil
		}); err != nil {
			log.Printf("消费者启动失败: %v", err)
		}
	}()
	// 等待终止信号
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	cancel()  // 通知消费者退出
	wg.Wait() // 等待消费者协程结束
}

// 豆包使用示例
func main() {
	// 生产者配置
	brokers := []string{"host.docker.internal:9092"}
	topic := "test-topic"

	// 创建生产者
	producer, err := kafka.NewProducer(brokers, topic)
	if err != nil {
		log.Fatalf("create producer failed: %v", err)
	}
	defer producer.Close()

	// 发送消息
	for i := 0; i < 20; i++ {
		if err := producer.SendMessage(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			log.Printf("send message failed: %v", err)
		}
	}

	// 消费者配置
	groupID := "test-group"

	// 创建消费者
	consumer, err := kafka.NewConsumer(brokers, topic, groupID)
	if err != nil {
		log.Fatalf("create consumer failed: %v", err)
	}
	defer consumer.Close()

	// 启动消费者
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Start(ctx, func(key, value string) error {
			log.Printf("received message: key=%s, value=%s", key, value)
			return nil // 业务处理逻辑
		}); err != nil {
			log.Printf("consumer error: %v", err)
		}
	}()
	wg.Wait()
}

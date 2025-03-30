package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Producer 生产者结构体
type Producer struct {
	producer sarama.AsyncProducer
	topic    string
}

// NewProducer 创建生产者实例
func NewProducer(brokers []string, topic string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0                         // 设置 Kafka 版本
	config.Producer.RequiredAcks = sarama.WaitForAll         // 等待所有副本确认
	config.Producer.Retry.Max = 5                            // 最大重试次数
	config.Producer.Retry.Backoff = 500 * time.Millisecond   // 重试间隔
	config.Producer.Compression = sarama.CompressionSnappy   // 压缩方式
	config.Producer.Flush.Frequency = 500 * time.Millisecond // 批量发送频率
	config.Producer.Flush.MaxMessages = 1000                 // 批量最大消息数

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("create producer error: %w", err)
	}

	return &Producer{
		producer: producer,
		topic:    topic,
	}, nil
}

// SendMessage 发送消息
func (p *Producer) SendMessage(key, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	select {
	case p.producer.Input() <- msg:
		return nil
	case err := <-p.producer.Errors():
		return fmt.Errorf("send message error: %w", err)
	}
}

// Close 关闭生产者
func (p *Producer) Close() error {
	return p.producer.Close()
}

// Consumer 消费者结构体
type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	topic         string
	groupID       string
}

// NewConsumer 创建消费者实例
func NewConsumer(brokers []string, topic, groupID string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0
	//config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin // 分区分配策略 高版本已取消
	//config.Consumer.AutoOffsetReset = sarama.OffsetOldest // 初始偏移量
	//config.Consumer.Fetch.MaxWaitTime = 200 * time.Millisecond                  // 等待数据时间
	//config.Consumer.Fetch.MinBytes = 1024                                       // 最小获取字节数
	config.Consumer.MaxWaitTime = 2 * time.Second // 最大等待时间

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("create consumer group error: %w", err)
	}

	return &Consumer{
		consumerGroup: consumerGroup,
		topic:         topic,
		groupID:       groupID,
	}, nil
}

// Start 启动消费者
func (c *Consumer) Start(ctx context.Context, handler func(key, value string) error) error {
	return c.consumerGroup.Consume(ctx, []string{c.topic}, &messageHandler{
		handler: handler,
	})
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	return c.consumerGroup.Close()
}

// messageHandler 实现 sarama.ConsumerGroupHandler 接口
type messageHandler struct {
	handler func(key, value string) error
}

func (h *messageHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *messageHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *messageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		err := h.handler(string(msg.Key), string(msg.Value))
		if err != nil {
			log.Printf("process message error: %v, message: %s", err, msg.Value)
			// 处理失败可重试或记录到死信队列
			continue
		}
		session.MarkMessage(msg, "") // 标记消息已处理
	}
	return nil
}

// 使用示例
func main() {
	// 生产者配置
	brokers := []string{"host.docker.internal:9092"}
	topic := "test-topic"

	// 创建生产者
	producer, err := NewProducer(brokers, topic)
	if err != nil {
		log.Fatalf("create producer failed: %v", err)
	}
	defer producer.Close()

	// 发送消息
	for i := 0; i < 10; i++ {
		if err := producer.SendMessage(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			log.Printf("send message failed: %v", err)
		}
	}

	// 消费者配置
	groupID := "test-group"

	// 创建消费者
	consumer, err := NewConsumer(brokers, topic, groupID)
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

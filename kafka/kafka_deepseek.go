package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// ================== Producer ==================

// ProducerConfig 生产者配置
type ProducerConfig struct {
	Brokers []string // Kafka 地址列表
	Topic   string   // 默认主题（可选）
}

// KafkaProducer 封装的生产者
type KafkaProducer struct {
	producer sarama.SyncProducer
	topic    string
	config   ProducerConfig
}

// NewKafkaProducer 创建新的生产者
func NewKafkaProducer(config ProducerConfig) (*KafkaProducer, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("至少需要一个 broker 地址")
	}

	// 配置生产者
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll // 确保消息被所有副本接收
	cfg.Producer.Retry.Max = 5                    // 重试次数
	cfg.Producer.Return.Successes = true          // 需要成功返回

	producer, err := sarama.NewSyncProducer(config.Brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建生产者失败: %v", err)
	}

	return &KafkaProducer{
		producer: producer,
		topic:    config.Topic,
		config:   config,
	}, nil
}

// SendMessage 发送消息
func (p *KafkaProducer) SendMessage(topic string, key, value []byte) error {
	if topic == "" {
		topic = p.topic
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("发送消息失败: %v", err)
	}

	log.Printf("消息发送成功! 主题: %s, 分区: %d, 偏移: %d", topic, partition, offset)
	return nil
}

// Close 关闭生产者
func (p *KafkaProducer) Close() error {
	return p.producer.Close()
}

// ================== Consumer ==================

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	Brokers    []string // Kafka 地址列表
	GroupID    string   // 消费者组ID
	Topics     []string // 消费主题列表
	AutoOffset string   // "oldest" 或 "newest"
	ShowLogs   bool     // 是否显示日志
}

// KafkaConsumer 封装的消费者
type KafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	config        ConsumerConfig
	handler       *consumerHandler
}

type consumerHandler struct {
	ready          chan bool
	messageHandler func(*sarama.ConsumerMessage) error
}

// NewKafkaConsumer 创建新的消费者组
func NewKafkaConsumer(config ConsumerConfig) (*KafkaConsumer, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("至少需要一个 broker 地址")
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V4_0_0_0 // 根据Kafka版本调整

	// 偏移量配置
	switch config.AutoOffset {
	case "oldest":
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		return nil, fmt.Errorf("无效的 AutoOffset 值")
	}

	consumerGroup, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建消费者组失败: %v", err)
	}

	return &KafkaConsumer{
		consumerGroup: consumerGroup,
		config:        config,
		handler: &consumerHandler{
			ready: make(chan bool),
		},
	}, nil
}

// Setup 消费者组启动前调用
func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

// Cleanup 消费者组结束时调用
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 消费消息
func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if h.messageHandler != nil {
			if err := h.messageHandler(message); err != nil {
				log.Printf("消息处理失败: %v", err)
				continue
			}
		}
		session.MarkMessage(message, "")
	}
	return nil
}

// Start 启动消费者（修复版）
func (kc *KafkaConsumer) Start(ctx context.Context, handler func(*sarama.ConsumerMessage) error) error {
	kc.handler.messageHandler = handler

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Println("收到终止信号，停止消费")
				return
			default:
				// 每次重新创建 context 避免使用已取消的上下文
				consumeCtx, _ := context.WithCancel(context.Background())
				if err := kc.consumerGroup.Consume(consumeCtx, kc.config.Topics, kc.handler); err != nil {
					if err == sarama.ErrClosedConsumerGroup {
						return
					}
					log.Printf("消费错误: %v", err)
					time.Sleep(5 * time.Second) // 添加重试间隔
				}
			}
		}
	}()

	<-kc.handler.ready
	log.Println("消费者已就绪")
	return nil
}

// Start2 启动消费者
func (kc *KafkaConsumer) Start2(ctx context.Context, handler func(*sarama.ConsumerMessage) error) error {
	kc.handler.messageHandler = handler

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := kc.consumerGroup.Consume(ctx, kc.config.Topics, kc.handler); err != nil {
				log.Printf("消费错误: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-kc.handler.ready
	log.Println("消费者已就绪")
	return nil
}

// Close 关闭消费者
func (kc *KafkaConsumer) Close() error {
	return kc.consumerGroup.Close()
}

// ================== 使用示例 ==================

func main2() {
	// 生产者示例
	producer, err := NewKafkaProducer(ProducerConfig{
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
	consumer, err := NewKafkaConsumer(ConsumerConfig{
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

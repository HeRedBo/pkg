package mq

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"go.uber.org/zap"
)

const (
	KafkaConsumerConnected    string = "connected"
	KafkaConsumerDisconnected string = "disconnected"
)

type Consumer struct {
	hosts    []string
	topics   []string
	config   *sarama.Config
	consumer sarama.ConsumerGroup
	status   string
	groupID  string

	handler    *consumerGroupHandler
	breaker    *breaker.Breaker
	reConnect  chan bool
	statusLock sync.Mutex
	exit       bool
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	log        Logger // 当前实例使用的 Logger
}

// KafkaMessageHandler  消费者回调函数
type KafkaMessageHandler func(message *sarama.ConsumerMessage) (bool, error)

type consumerGroupHandler struct {
	ready    chan bool
	callback KafkaMessageHandler
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if commit, err := h.callback(message); commit {
			session.MarkMessage(message, "") // 标记消息已处理
		} else if err != nil {
			//logger.Error("kafka consumer msg error ", zap.Error(err))
		}
	}
	return nil
}

func getKafkaDefaultConsumerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	config.Consumer.Fetch.Default = 1024 * 1024 * 2
	return config
}

// StartKafkaConsumer 启动消费者
// opts 可选：WithLogger(l) 注入文件日志；不传则按 全局SetLogger > 默认控制台 优先级选取
func StartKafkaConsumer(hosts, topics []string, groupID string, config *sarama.Config, f KafkaMessageHandler, opts ...Option) (*Consumer, error) {
	o := applyOptions(opts)
	if config == nil {
		config = getKafkaDefaultConsumerConfig()
	}

	consumer := &Consumer{
		hosts:   hosts,
		config:  config,
		status:  KafkaConsumerDisconnected,
		groupID: groupID,
		topics:  topics,
		handler: &consumerGroupHandler{
			ready:    make(chan bool),
			callback: f,
		},
		breaker:   breaker.New(3, 1, 3*time.Second),
		reConnect: make(chan bool),
		log:       getLogger(o.logger),
	}

	if err := consumer.connect(); err != nil {
		return nil, err
	}

	go consumer.keepConnect()
	go consumer.consume()

	return consumer, nil
}

func (c *Consumer) connect() error {
	var err error
	c.consumer, err = sarama.NewConsumerGroup(c.hosts, c.groupID, c.config)
	if err != nil {
		return err
	}
	c.status = KafkaConsumerConnected
	c.log.Info("kafka consumer started", zap.String("groupID", c.groupID), zap.Strings("topics", c.topics))
	return nil
}

func (c *Consumer) Close() error {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	c.exit = true
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	return c.consumer.Close()
}

func (c *Consumer) keepConnect() {
	l := c.log
	// 外层循环：持续监听重连信号，直到 exit 标志为 true
	for !c.exit {
		select {
		case <-c.reConnect: // 监听重连信号通道
			// 1. 检查当前状态是否为已断开
			if c.status != KafkaConsumerDisconnected {
				continue // 如果已连接，跳过后续逻辑
			}
			l.Warn("kafka consumer reconnecting", zap.String("groupID", c.groupID), zap.Strings("topics", c.topics))
		breakLoop: // 标签：用于跳出内部重试循环
			// 2. 内部重试循环：尝试重连直到成功或熔断器开启
			for {
				// 2.1 通过熔断器执行连接操作
				err := c.breaker.Run(func() error {
					return c.connect() // 尝试连接 Kafka
				})
				// 2.2 根据错误类型处理结果
				switch err {
				case nil: // 成功连接
					go c.consume()  // 启动消息消费协程
					break breakLoop // 跳出内部重试循环
				case breaker.ErrBreakerOpen: // 熔断器开启（频繁失败）
					l.Warn("kafka consumer breaker open, will retry after 5s",
						zap.String("groupID", c.groupID))
					// 5秒后重新触发重连信号
					time.AfterFunc(5*time.Second, func() { c.reConnect <- true })
					break breakLoop // 跳出内部重试循环
				default: // 其他错误（如网络问题）
					l.Error("kafka consumer connect error", zap.String("groupID", c.groupID), zap.Error(err))
				}
			}
		}
	}
}

func (c *Consumer) consume() {
	l := c.log
	// 1. 创建可取消的上下文，用于控制消费者生命周期
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel // 保存 cancel 函数，供外部调用关闭
	// 2. 启动一个协程处理消息消费
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			// 2.1 核心消费循环：调用 Kafka 的 Consume 方法
			if err := c.consumer.Consume(ctx, c.topics, c.handler); err != nil {
				// 2.1.1 如果是正常关闭错误，直接退出
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				l.Error("kafka consumer error", zap.String("groupID", c.groupID), zap.Error(err))
				// 2.1.2 处理其他错误（如网络中断）
				c.handleConsumerError(err)
			}

			// 2.2 检查上下文是否已关闭，是则退出循环
			if ctx.Err() != nil {
				return
			}
		}
	}()
	// 3. 等待消费者组初始化完成（handler.Setup() 被调用）
	<-c.handler.ready // 等待消费者组初始化完成
	l.Info("kafka consumer ready", zap.String("groupID", c.groupID), zap.Strings("topics", c.topics))
	// 4. 监听系统终止信号（SIGINT/SIGTERM）
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	// 5. 使用 select 监听两个事件通道
	select {
	case <-ctx.Done(): // 5.1 上下文被取消（如主动调用 Close()）
		l.Info("kafka consumer context closed", zap.String("groupID", c.groupID))
	case <-sigterm: // 阻塞直到接收到终止信号
		l.Warn("kafka consumer received termination signal", zap.String("groupID", c.groupID))
		cancel() // 触发上下文取消
	}
}

func (c *Consumer) handleConsumerError(err error) {
	if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
		c.statusLock.Lock()
		if c.status == KafkaConsumerConnected {
			c.status = KafkaConsumerDisconnected
			c.log.Warn("kafka consumer disconnected, triggering reconnect",
				zap.String("groupID", c.groupID), zap.Error(err))
			c.reConnect <- true
		}
		c.statusLock.Unlock()
	}
}

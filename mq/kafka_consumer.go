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
	KafkaConsumerClosed       string = "closed"
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
	exit       chan struct{} // 统一退出广播通道
	closed     bool          // Close 已调用标记
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
		exit:      make(chan struct{}),
		log:       getLogger(o.logger),
	}

	if err := consumer.connect(); err != nil {
		return nil, err
	}

	go consumer.watchSignals()
	go consumer.keepConnect()
	go consumer.consume()

	return consumer, nil
}

// watchSignals 统一信号监听，收到信号后广播退出
func (c *Consumer) watchSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	select {
	case <-signals:
		c.statusLock.Lock()
		if !c.closed {
			c.status = KafkaConsumerClosed
		}
		c.statusLock.Unlock()
		close(c.exit)
	case <-c.exit:
		// 已经退出（可能通过 Close 触发）
	}
}

func (c *Consumer) connect() error {
	var err error
	c.consumer, err = sarama.NewConsumerGroup(c.hosts, c.groupID, c.config)
	if err != nil {
		return err
	}
	c.statusLock.Lock()
	c.status = KafkaConsumerConnected
	c.statusLock.Unlock()
	c.log.Info("kafka consumer started", zap.String("groupID", c.groupID), zap.Strings("topics", c.topics))
	return nil
}

func (c *Consumer) Close() error {
	c.statusLock.Lock()
	if c.closed {
		c.statusLock.Unlock()
		return nil
	}
	c.closed = true
	close(c.exit) // 广播退出
	if c.cancel != nil {
		c.cancel()
	}
	c.statusLock.Unlock()
	c.wg.Wait()
	return c.consumer.Close()
}

func (c *Consumer) keepConnect() {
	l := c.log
	for {
		select {
		case <-c.exit:
			l.Debug("consumer keepConnect exited", zap.String("groupID", c.groupID))
			return
		case <-c.reConnect:
			c.statusLock.Lock()
			closed := c.closed
			disconnected := c.status == KafkaConsumerDisconnected
			c.statusLock.Unlock()
			if closed || !disconnected {
				break
			}
			l.Warn("kafka consumer reconnecting", zap.String("groupID", c.groupID), zap.Strings("topics", c.topics))
		breakLoop:
			for {
				err := c.breaker.Run(func() error {
					return c.connect()
				})
				switch err {
				case nil:
					go c.consume()
					break breakLoop
				case breaker.ErrBreakerOpen:
					l.Warn("kafka consumer breaker open, will retry after 5s",
						zap.String("groupID", c.groupID))
					c.statusLock.Lock()
					shouldRetry := c.status == KafkaConsumerDisconnected && !c.closed
					c.statusLock.Unlock()
					if shouldRetry {
						time.AfterFunc(5*time.Second, func() {
							select {
							case c.reConnect <- true:
							case <-c.exit:
							}
						})
					}
					break breakLoop
				default:
					l.Error("kafka consumer connect error", zap.String("groupID", c.groupID), zap.Error(err))
				}
			}
		}
	}
}

func (c *Consumer) consume() {
	l := c.log

	// 重建 handler，防止 handler.ready 被重复 close
	c.statusLock.Lock()
	c.handler = &consumerGroupHandler{
		ready:    make(chan bool),
		callback: c.handler.callback,
	}
	handler := c.handler
	c.statusLock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	c.statusLock.Lock()
	c.cancel = cancel
	c.statusLock.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if err := c.consumer.Consume(ctx, c.topics, handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				l.Error("kafka consumer error", zap.String("groupID", c.groupID), zap.Error(err))
				c.handleConsumerError(err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	<-handler.ready
	l.Info("kafka consumer ready", zap.String("groupID", c.groupID), zap.Strings("topics", c.topics))

	// 使用 exit 通道替代独立信号监听
	select {
	case <-ctx.Done():
		l.Info("kafka consumer context closed", zap.String("groupID", c.groupID))
	case <-c.exit:
		l.Warn("kafka consumer received exit signal", zap.String("groupID", c.groupID))
		cancel()
	}
}

func (c *Consumer) handleConsumerError(err error) {
	if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
		c.statusLock.Lock()
		if c.status == KafkaConsumerConnected {
			c.status = KafkaConsumerDisconnected
			c.log.Warn("kafka consumer disconnected, triggering reconnect",
				zap.String("groupID", c.groupID), zap.Error(err))
			select {
			case c.reConnect <- true:
			case <-c.exit:
			}
		}
		c.statusLock.Unlock()
	}
}

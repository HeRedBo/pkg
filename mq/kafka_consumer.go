package mq

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/eapache/go-resiliency/breaker"
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
			session.MarkMessage(message, "")
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
func StartKafkaConsumer(hosts, topics []string, groupID string, config *sarama.Config, f KafkaMessageHandler) (*Consumer, error) {
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
	//logger.Info("kafka consumer started", zap.Any(c.groupID, c.topics))
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
	for !c.exit {
		select {
		case <-c.reConnect:
			if c.status != KafkaConsumerDisconnected {
				continue
			}

			//logger.Warn("KafkaConsumer reconnecting", zap.Any(c.groupID, c.topics))
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
					time.AfterFunc(5*time.Second, func() { c.reConnect <- true })
					break breakLoop
				default:
					//logger.Error("kafka consumer connect error", zap.Error(err))
				}
			}
		}
	}
}

func (c *Consumer) consume() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			if err := c.consumer.Consume(ctx, c.topics, c.handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				//logger.Error("kafka consumer error", zap.Error(err))
				c.handleConsumerError(err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-c.handler.ready

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		//logger.Info("kafka consumer context closed")
	case <-sigterm:
		//logger.Info("kafka consumer received termination signal")
		cancel()
	}
}

func (c *Consumer) handleConsumerError(err error) {
	if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
		c.statusLock.Lock()
		if c.status == KafkaConsumerConnected {
			c.status = KafkaConsumerDisconnected
			c.reConnect <- true
		}
		c.statusLock.Unlock()
	}
}

//go:build integration

package test

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HeRedBo/pkg/mq"
	"github.com/IBM/sarama"
	"go.uber.org/zap/zapcore"
)

// ─────────────────────────────────────────────
// 消费者集成测试
// 前置：Kafka 运行在 127.0.0.1:9092，topic=test-topic 已存在
// ─────────────────────────────────────────────

// TestConsumer_StartAndReceive
// 先启动消费者（等待 ready），再发消息，验证回调被触发
// 注意：使用 OffsetNewest 配置，必须先启消费者后发消息，否则消息会被跳过
func TestConsumer_StartAndReceive(t *testing.T) {
	const groupID = "test-group-integration"

	// 1. 先初始化同步生产者（不发消息）
	const pName = "consumer-test-sync-producer"
	if err := mq.InitSyncKafkaProducer(pName, testHosts, nil); err != nil {
		t.Fatalf("init sync producer failed: %v", err)
	}
	p := mq.GetKafkaSyncProducer(pName)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, pName)
	})

	// 2. 先启动消费者，等待其 ready（已建立 session）
	var received int32 // 原子计数器，记录收到的消息数

	consumer, err := mq.StartKafkaConsumer(
		testHosts,
		[]string{testTopic},
		groupID,
		nil,
		func(message *sarama.ConsumerMessage) (bool, error) {
			t.Logf("received message => topic=%s partition=%d offset=%d value=%s",
				message.Topic, message.Partition, message.Offset, string(message.Value))
			atomic.AddInt32(&received, 1)
			return true, nil // commit offset
		},
	)
	if err != nil {
		t.Fatalf("StartKafkaConsumer failed: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	// 3. 等待消费者完成 rebalance，处于 ready 状态后再发消息
	// StartKafkaConsumer 内部 <-handler.ready 之后才会打 "kafka consumer ready"
	// 这里额外等 1s 保证 session 完全建立
	time.Sleep(3 * time.Second)
	t.Log("consumer is ready, now sending message")

	// 4. 消费者 ready 后再发消息
	body, _ := json.Marshal(testMsg{ID: 9001, Name: "consumer test", CreateAt: time.Now().Unix()})
	_, _, err = p.Send(&sarama.ProducerMessage{
		Topic:     testTopic,
		Value:     mq.KafkaMsgValueEncoder(body),
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("send message failed: %v", err)
	}
	t.Log("producer sent 1 message")

	// 5. 等待最多 10 秒，验证至少收到 1 条消息
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&received) >= 1 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if atomic.LoadInt32(&received) == 0 {
		t.Error("consumer: expected to receive at least 1 message within 10s, got 0")
	} else {
		t.Logf("consumer: received %d message(s)", atomic.LoadInt32(&received))
	}
}

// TestConsumer_Close 验证消费者可以正常关闭，不 panic
func TestConsumer_Close(t *testing.T) {
	const groupID = "test-group-close"

	consumer, err := mq.StartKafkaConsumer(
		testHosts,
		[]string{testTopic},
		groupID,
		nil,
		func(message *sarama.ConsumerMessage) (bool, error) {
			return true, nil
		},
	)
	if err != nil {
		t.Fatalf("StartKafkaConsumer failed: %v", err)
	}

	// 稍等片刻让消费者初始化完成
	time.Sleep(2 * time.Second)

	if err := consumer.Close(); err != nil {
		t.Errorf("consumer.Close() returned error: %v", err)
	}
	t.Log("consumer closed cleanly")
}

// TestConsumer_WithZapLogger 验证消费者注入 zap.Logger 后初始化日志可观测
func TestConsumer_WithZapLogger(t *testing.T) {
	zapL, logs := newObserverZap(zapcore.DebugLevel)
	const groupID = "test-group-zap-logger"

	consumer, err := mq.StartKafkaConsumer(
		testHosts,
		[]string{testTopic},
		groupID,
		nil,
		func(message *sarama.ConsumerMessage) (bool, error) { return true, nil },
		mq.WithLogger(zapL),
	)
	if err != nil {
		t.Fatalf("StartKafkaConsumer with zap logger failed: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	// connect() 成功后会打 "kafka consumer started"
	time.Sleep(2 * time.Second)

	if logs.Len() == 0 {
		t.Fatal("expected log entries after consumer start, got 0")
	}

	found := false
	for _, e := range logs.All() {
		t.Logf("  log[%s] %s", e.Level, e.Message)
		if e.Message == "kafka consumer started" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'kafka consumer started' info log")
	}
}

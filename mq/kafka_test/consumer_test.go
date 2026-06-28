package kafka_test

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
// 消费者测试
// ─────────────────────────────────────────────

// TestConsumerReceive 启动消费者 → 用同步生产者发送测试消息 → 验证消费者回调收到消息 → 关闭
func TestConsumerReceive(t *testing.T) {
	checkKafkaAvailable(t)

	const (
		topic   = "test-consumer-topic"
		groupID = "test-group"
		pName   = "consumer-test-sync-producer"
	)

	// 1. 初始化同步生产者（用于发送测试消息）
	if err := mq.InitSyncKafkaProducer(pName, testHosts, nil); err != nil {
		t.Fatalf("init sync producer failed: %v", err)
	}
	p := mq.GetKafkaSyncProducer(pName)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, pName)
	})

	// 2. 启动消费者，使用 channel + timeout 机制验证消息
	var received int32
	msgCh := make(chan *sarama.ConsumerMessage, 10)

	consumer, err := mq.StartKafkaConsumer(
		testHosts,
		[]string{topic},
		groupID,
		nil,
		func(message *sarama.ConsumerMessage) (bool, error) {
			t.Logf("received => topic=%s partition=%d offset=%d value=%s",
				message.Topic, message.Partition, message.Offset, string(message.Value))
			atomic.AddInt32(&received, 1)
			select {
			case msgCh <- message:
			default:
			}
			return true, nil
		},
	)
	if err != nil {
		t.Fatalf("StartKafkaConsumer failed: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	// 3. 等待消费者完成 rebalance，确保 session 建立后再发消息
	time.Sleep(3 * time.Second)
	t.Log("consumer is ready, sending test message")

	// 4. 发送测试消息
	msg := testMsg{ID: 100, Name: "consumer test", CreateAt: time.Now().Unix()}
	body, _ := json.Marshal(msg)
	_, _, err = p.Send(&sarama.ProducerMessage{
		Topic:     topic,
		Value:     mq.KafkaMsgValueEncoder(body),
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("send test message failed: %v", err)
	}
	t.Log("producer sent 1 test message")

	// 5. 使用 channel + timeout 等待消息
	select {
	case m := <-msgCh:
		t.Logf("consumer received message: topic=%s, partition=%d, offset=%d",
			m.Topic, m.Partition, m.Offset)
	case <-time.After(10 * time.Second):
		if atomic.LoadInt32(&received) == 0 {
			t.Error("consumer: expected to receive at least 1 message within 10s, got 0")
		}
	}
}

// ─────────────────────────────────────────────
// 使用 WithLogger 注入自定义日志的消费者测试
// ─────────────────────────────────────────────

// TestConsumerWithCustomLogger 使用 WithLogger 注入自定义日志的消费者测试
func TestConsumerWithCustomLogger(t *testing.T) {
	checkKafkaAvailable(t)

	zapL, logs := newObserverZap(zapcore.DebugLevel)

	const (
		topic   = "test-consumer-topic"
		groupID = "test-group-zap"
	)

	consumer, err := mq.StartKafkaConsumer(
		testHosts,
		[]string{topic},
		groupID,
		nil,
		func(message *sarama.ConsumerMessage) (bool, error) { return true, nil },
		mq.WithLogger(zapL),
	)
	if err != nil {
		t.Fatalf("StartKafkaConsumer with zap logger failed: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	// 等待消费者初始化完成
	time.Sleep(2 * time.Second)

	// 验证日志中有 "kafka consumer started"
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


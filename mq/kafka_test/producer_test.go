package kafka_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/HeRedBo/pkg/mq"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// ─────────────────────────────────────────────
// 测试辅助
// ─────────────────────────────────────────────

// kafkaAddr Kafka 地址，默认 localhost:9092，可通过环境变量 KAFKA_ADDR 覆盖
var kafkaAddr = func() string {
	if addr := os.Getenv("KAFKA_ADDR"); addr != "" {
		return addr
	}
	return "localhost:9092"
}()

// checkKafkaAvailable 尝试连接 Kafka，失败则跳过测试
func checkKafkaAvailable(t *testing.T) {
	t.Helper()
	conf := sarama.NewConfig()
	conf.Net.DialTimeout = 3 * time.Second
	client, err := sarama.NewClient(testHosts, conf)
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", kafkaAddr, err)
		return
	}
	client.Close()
}

// testHosts 测试用 broker 列表
var testHosts = []string{kafkaAddr}

// testMsg 测试消息结构体
type testMsg struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	CreateAt int64  `json:"create_at"`
}

// newObserverZap 创建一个写入内存的 zap.Logger（用于断言日志输出）
func newObserverZap(lvl zapcore.Level) (*zap.Logger, *observer.ObservedLogs) {
	core, logs := observer.New(lvl)
	return zap.New(core), logs
}

// ─────────────────────────────────────────────
// 同步生产者测试
// ─────────────────────────────────────────────

// TestSyncProducerSend 初始化同步生产者 → 发送消息 → 验证无错误 → 关闭
func TestSyncProducerSend(t *testing.T) {
	checkKafkaAvailable(t)

	const name = "test-sync-producer"
	err := mq.InitSyncKafkaProducer(name, testHosts, nil)
	if err != nil {
		t.Fatalf("InitSyncKafkaProducer failed: %v", err)
	}

	p := mq.GetKafkaSyncProducer(name)
	if p == nil {
		t.Fatal("GetKafkaSyncProducer returned nil")
	}
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, name)
	})

	msg := testMsg{ID: 1, Name: "sync test", CreateAt: time.Now().Unix()}
	body, _ := json.Marshal(msg)

	partition, offset, err := p.Send(&sarama.ProducerMessage{
		Topic:     "test-sync-topic",
		Value:     mq.KafkaMsgValueEncoder(body),
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	t.Logf("message sent => partition=%d, offset=%d", partition, offset)
	if partition < 0 {
		t.Errorf("invalid partition: %d", partition)
	}
	if offset < 0 {
		t.Errorf("invalid offset: %d", offset)
	}
}

// ─────────────────────────────────────────────
// 异步生产者测试
// ─────────────────────────────────────────────

// TestAsyncProducerSend 初始化异步生产者 → 发送消息 → 关闭
func TestAsyncProducerSend(t *testing.T) {
	checkKafkaAvailable(t)

	const name = "test-async-producer"
	err := mq.InitAsyncKafkaProducer(name, testHosts, nil)
	if err != nil {
		t.Fatalf("InitAsyncKafkaProducer failed: %v", err)
	}

	p := mq.GetKafkaAsyncProducer(name)
	if p == nil {
		t.Fatal("GetKafkaAsyncProducer returned nil")
	}
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaAsyncProducers, name)
	})

	msg := testMsg{ID: 2, Name: "async test", CreateAt: time.Now().Unix()}
	body, _ := json.Marshal(msg)

	err = p.Send(&sarama.ProducerMessage{
		Topic: "test-async-topic",
		Value: mq.KafkaMsgValueEncoder(body),
	})
	if err != nil {
		t.Fatalf("async Send failed: %v", err)
	}

	// 异步投递需要等待 broker ack
	time.Sleep(2 * time.Second)
	t.Log("async message submitted successfully")
}

// ─────────────────────────────────────────────
// 同步生产者批量发送测试
// ─────────────────────────────────────────────

// TestSyncProducerBatchSend 批量发送消息测试
func TestSyncProducerBatchSend(t *testing.T) {
	checkKafkaAvailable(t)

	const name = "test-sync-batch-producer"
	err := mq.InitSyncKafkaProducer(name, testHosts, nil)
	if err != nil {
		t.Fatalf("InitSyncKafkaProducer failed: %v", err)
	}

	p := mq.GetKafkaSyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, name)
	})

	var msgs []*sarama.ProducerMessage
	for i := int64(1); i <= 5; i++ {
		body, _ := json.Marshal(testMsg{ID: i, Name: "batch msg", CreateAt: time.Now().Unix()})
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: "test-sync-topic",
			Value: mq.KafkaMsgValueEncoder(body),
		})
	}

	errs := p.SendMessages(msgs)
	if len(errs) > 0 {
		t.Fatalf("SendMessages errors: %v", errs)
	}
	t.Logf("batch of %d messages sent successfully", len(msgs))
}

// ─────────────────────────────────────────────
// 使用 WithLogger 注入自定义日志的生产者测试
// ─────────────────────────────────────────────

// TestProducerWithCustomLogger 使用 WithLogger 注入自定义日志的生产者测试
func TestProducerWithCustomLogger(t *testing.T) {
	checkKafkaAvailable(t)

	zapL, logs := newObserverZap(zapcore.DebugLevel)

	const name = "test-sync-zap-producer"
	err := mq.InitSyncKafkaProducer(name, testHosts, nil, mq.WithLogger(zapL))
	if err != nil {
		t.Fatalf("InitSyncKafkaProducer with zap logger failed: %v", err)
	}

	p := mq.GetKafkaSyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, name)
	})

	// 初始化成功后应至少有一条日志 "SyncKafkaProducer connected"
	if logs.Len() < 1 {
		t.Fatalf("expected at least 1 log entry after init, got %d", logs.Len())
	}

	found := false
	for _, e := range logs.All() {
		if e.Level == zapcore.InfoLevel && e.Message == "SyncKafkaProducer connected" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected 'SyncKafkaProducer connected' info log, got: %+v", logs.All())
	}

	// 发送一条消息验证生产者正常工作
	msg := testMsg{ID: 3, Name: "zap logger test", CreateAt: time.Now().Unix()}
	body, _ := json.Marshal(msg)
	_, _, err = p.Send(&sarama.ProducerMessage{
		Topic:     "test-sync-topic",
		Value:     mq.KafkaMsgValueEncoder(body),
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Send with custom logger failed: %v", err)
	}
	t.Log("producer with custom logger works correctly")
}

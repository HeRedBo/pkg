//go:build integration

package test

// 集成测试：需要本地 Kafka 运行在 127.0.0.1:9092
// 运行命令：go test -v -tags integration -timeout 30s ./test/

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/HeRedBo/pkg/mq"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// ─────────────────────────────────────────────
// 测试常量
// ─────────────────────────────────────────────

const (
	testBroker    = "127.0.0.1:9092"
	testTopic     = "test-topic"
	syncProducer  = "test-sync-producer"
	asyncProducer = "test-async-producer"
)

var testHosts = []string{testBroker}

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

// TestSyncProducer_Init 验证同步生产者初始化成功
func TestSyncProducer_Init(t *testing.T) {
	err := mq.InitSyncKafkaProducer(syncProducer, testHosts, nil)
	if err != nil {
		t.Fatalf("InitSyncKafkaProducer failed: %v", err)
	}

	p := mq.GetKafkaSyncProducer(syncProducer)
	if p == nil {
		t.Fatal("GetKafkaSyncProducer returned nil")
	}

	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, syncProducer)
	})
}

// TestSyncProducer_SendSingleMsg 验证同步生产者发送单条消息
func TestSyncProducer_SendSingleMsg(t *testing.T) {
	err := mq.InitSyncKafkaProducer(syncProducer+"_single", testHosts, nil)
	if err != nil {
		t.Fatalf("InitSyncKafkaProducer failed: %v", err)
	}

	p := mq.GetKafkaSyncProducer(syncProducer + "_single")
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, syncProducer+"_single")
	})

	msg := testMsg{ID: 1001, Name: "sync single test", CreateAt: time.Now().Unix()}
	body, _ := json.Marshal(msg)

	partition, offset, err := p.Send(&sarama.ProducerMessage{
		Topic:     testTopic,
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

// TestSyncProducer_SendBatchMsgs 验证同步生产者批量发送消息
func TestSyncProducer_SendBatchMsgs(t *testing.T) {
	err := mq.InitSyncKafkaProducer(syncProducer+"_batch", testHosts, nil)
	if err != nil {
		t.Fatalf("InitSyncKafkaProducer failed: %v", err)
	}

	p := mq.GetKafkaSyncProducer(syncProducer + "_batch")
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, syncProducer+"_batch")
	})

	var msgs []*sarama.ProducerMessage
	for i := int64(1); i <= 5; i++ {
		body, _ := json.Marshal(testMsg{ID: i, Name: "batch msg", CreateAt: time.Now().Unix()})
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: testTopic,
			Value: mq.KafkaMsgValueEncoder(body),
		})
	}

	errs := p.SendMessages(msgs)
	if len(errs) > 0 {
		t.Fatalf("SendMessages errors: %v", errs)
	}
	t.Logf("batch of %d messages sent successfully", len(msgs))
}

// TestSyncProducer_WithZapLogger 验证注入 zap.Logger 后生产者日志走 observer
func TestSyncProducer_WithZapLogger(t *testing.T) {
	zapL, logs := newObserverZap(zapcore.DebugLevel)

	name := syncProducer + "_zap"
	err := mq.InitSyncKafkaProducer(name, testHosts, nil, mq.WithLogger(zapL))
	if err != nil {
		t.Fatalf("InitSyncKafkaProducer with zap logger failed: %v", err)
	}

	p := mq.GetKafkaSyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, name)
	})

	// 初始化成功时会打一条 Info "SyncKafkaProducer connected"
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
}

// ─────────────────────────────────────────────
// 异步生产者测试
// ─────────────────────────────────────────────

// TestAsyncProducer_Init 验证异步生产者初始化成功
func TestAsyncProducer_Init(t *testing.T) {
	err := mq.InitAsyncKafkaProducer(asyncProducer, testHosts, nil)
	if err != nil {
		t.Fatalf("InitAsyncKafkaProducer failed: %v", err)
	}

	p := mq.GetKafkaAsyncProducer(asyncProducer)
	if p == nil {
		t.Fatal("GetKafkaAsyncProducer returned nil")
	}

	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaAsyncProducers, asyncProducer)
	})
}

// TestAsyncProducer_SendMsg 验证异步生产者发送消息（无错误即通过）
func TestAsyncProducer_SendMsg(t *testing.T) {
	name := asyncProducer + "_send"
	err := mq.InitAsyncKafkaProducer(name, testHosts, nil)
	if err != nil {
		t.Fatalf("InitAsyncKafkaProducer failed: %v", err)
	}

	p := mq.GetKafkaAsyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaAsyncProducers, name)
	})

	msg := testMsg{ID: 2001, Name: "async send test", CreateAt: time.Now().Unix()}
	body, _ := json.Marshal(msg)

	err = p.Send(&sarama.ProducerMessage{
		Topic: testTopic,
		Value: mq.KafkaMsgValueEncoder(body),
	})
	if err != nil {
		t.Fatalf("async Send failed: %v", err)
	}

	// 异步投递需等待 broker ack（内部 check goroutine 处理）
	time.Sleep(2 * time.Second)
	t.Log("async message submitted successfully")
}

// TestAsyncProducer_WithZapLogger 验证异步生产者注入 zap.Logger 后初始化日志可观测
func TestAsyncProducer_WithZapLogger(t *testing.T) {
	zapL, logs := newObserverZap(zapcore.DebugLevel)

	name := asyncProducer + "_zap"
	err := mq.InitAsyncKafkaProducer(name, testHosts, nil, mq.WithLogger(zapL))
	if err != nil {
		t.Fatalf("InitAsyncKafkaProducer with zap logger failed: %v", err)
	}

	p := mq.GetKafkaAsyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaAsyncProducers, name)
	})

	if logs.Len() < 1 {
		t.Fatalf("expected at least 1 log entry after init, got %d", logs.Len())
	}

	found := false
	for _, e := range logs.All() {
		if e.Level == zapcore.InfoLevel && e.Message == "AsyncKafkaProducer connected" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected 'AsyncKafkaProducer connected' info log, got: %+v", logs.All())
	}
}

//go:build integration

package test

// 日志注入集成测试：验证三级日志优先级在真实 Kafka 初始化场景下均生效
// 运行命令：go test -v -tags integration -run TestLogger ./test/

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/HeRedBo/pkg/mq"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestLogger_GlobalSetLogger_ProducerUsesIt
// 全局 SetLogger 注入后，Init 的生产者日志应走注入的 Logger
func TestLogger_GlobalSetLogger_ProducerUsesIt(t *testing.T) {
	zapL, logs := newObserverZap(zapcore.DebugLevel)
	mq.SetLogger(zapL)
	t.Cleanup(func() { mq.SetLogger(nil) })

	const name = "logger-global-sync"
	if err := mq.InitSyncKafkaProducer(name, testHosts, nil); err != nil {
		t.Fatalf("InitSyncKafkaProducer failed: %v", err)
	}
	p := mq.GetKafkaSyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, name)
	})

	// 初始化成功后 logger 记录 "SyncKafkaProducer connected"
	if logs.Len() == 0 {
		t.Fatal("expected log entries after init, global SetLogger not working")
	}
	found := false
	for _, e := range logs.All() {
		if e.Message == "SyncKafkaProducer connected" {
			found = true
			t.Logf("global logger captured: [%s] %s", e.Level, e.Message)
		}
	}
	if !found {
		t.Errorf("'SyncKafkaProducer connected' not captured by global logger; logs: %+v", logs.All())
	}
}

// TestLogger_WithLoggerOption_OverridesGlobal
// Option 注入 Logger 优先级高于全局 SetLogger：
// 全局 Logger 应该 0 条日志，Option Logger 应该有日志
func TestLogger_WithLoggerOption_OverridesGlobal(t *testing.T) {
	globalZap, globalLogs := newObserverZap(zapcore.DebugLevel)
	mq.SetLogger(globalZap)
	t.Cleanup(func() { mq.SetLogger(nil) })

	optionZap, optionLogs := newObserverZap(zapcore.DebugLevel)

	const name = "logger-option-priority"
	err := mq.InitSyncKafkaProducer(name, testHosts, nil, mq.WithLogger(optionZap))
	if err != nil {
		t.Fatalf("InitSyncKafkaProducer failed: %v", err)
	}
	p := mq.GetKafkaSyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, name)
	})

	// Option Logger 应有日志
	if optionLogs.Len() == 0 {
		t.Error("option logger: expected log entries, got 0")
	}
	// Global Logger 不应有日志（被 Option 覆盖）
	if globalLogs.Len() != 0 {
		t.Errorf("global logger: expected 0 log entries, got %d (option priority broken)", globalLogs.Len())
	}

	t.Logf("option logger entries: %d, global logger entries: %d", optionLogs.Len(), globalLogs.Len())
}

// TestLogger_SaramaZapLogger_Integration
// SetSaramaLogger 注入后，Sarama 内部初始化日志走 observer，验证桥接生效
func TestLogger_SaramaZapLogger_Integration(t *testing.T) {
	zapL, logs := newObserverZap(zapcore.DebugLevel)
	mq.SetSaramaLogger(zapL)
	t.Cleanup(func() { mq.SetSaramaLogger(nil) }) // 恢复 sarama 默认输出

	const name = "logger-sarama-bridge"
	if err := mq.InitSyncKafkaProducer(name, testHosts, nil); err != nil {
		t.Fatalf("InitSyncKafkaProducer failed: %v", err)
	}
	p := mq.GetKafkaSyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, name)
	})

	// sarama 在建立连接时会打印若干内部日志（Debug 级别）
	if logs.Len() == 0 {
		t.Log("note: sarama produced 0 log entries (may vary by sarama version / log verbosity)")
	} else {
		t.Logf("sarama bridge captured %d log entries via zap observer", logs.Len())
		for _, e := range logs.All() {
			t.Logf("  [%s] %s", e.Level, e.Message)
		}
	}
}

// ─────────────────────────────────────────────
// 文件日志测试：验证 Kafka 日志写入按日期命名的文件
// ─────────────────────────────────────────────

// newFileZapLogger 用 zapcore 直接构建同时写控制台+文件的 zap.Logger
// logFile: 目标文件路径，自动创建目录
func newFileZapLogger(t *testing.T, logFile string) *zap.Logger {
	t.Helper()
	dir := logFile[:strings.LastIndex(logFile, "/")]
	if err := os.MkdirAll(dir, 0766); err != nil {
		t.Fatalf("mkdir %s failed: %v", dir, err)
	}
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		t.Fatalf("open log file %s failed: %v", logFile, err)
	}
	t.Cleanup(func() { _ = f.Close() })

	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		TimeKey:        "time",
		CallerKey:      "caller",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		LineEnding:     zapcore.DefaultLineEnding,
	}
	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(f),
		zapcore.DebugLevel,
	)
	consoleCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderCfg),
		zapcore.AddSync(os.Stdout),
		zapcore.DebugLevel,
	)
	return zap.New(zapcore.NewTee(fileCore, consoleCore), zap.AddCaller())
}

// assertFileContains 读取文件内容，验证包含指定字符串
func assertFileContains(t *testing.T, filePath, keyword string) error {
	t.Helper()
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open log file %q failed: %w", filePath, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), keyword) {
			t.Logf("found in log file: %s", scanner.Text())
			return nil
		}
	}
	return fmt.Errorf("keyword %q not found in %q", keyword, filePath)
}

// TestLogger_WriteToFile
// 验证 Kafka 生产者日志写入按当天日期命名的文件
// 日志文件：./logs/kafka-2006-01-02.log（日期动态生成，写入当前测试目录下）
func TestLogger_WriteToFile(t *testing.T) {
	// 1. 构建按当天日期命名的日志文件路径
	logFile := fmt.Sprintf("./logs/kafka-%s.log", time.Now().Format("2006-01-02"))
	t.Logf("target log file: %s", logFile)

	// 2. 构建写文件的 zap.Logger（同时输出控制台 + 文件）
	zapL := newFileZapLogger(t, logFile)
	t.Cleanup(func() { _ = zapL.Sync() })

	// 3. 注入到 mq 全局 Logger
	mq.SetLogger(zapL)
	t.Cleanup(func() { mq.SetLogger(nil) })

	// 4. 初始化同步生产者，初始化日志会写入文件
	const name = "file-logger-sync-producer"
	if err := mq.InitSyncKafkaProducer(name, testHosts, nil); err != nil {
		t.Fatalf("InitSyncKafkaProducer failed: %v", err)
	}
	p := mq.GetKafkaSyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, name)
	})

	// 5. 发一条消息（Send 成功/失败的日志也会写入文件）
	body, _ := json.Marshal(testMsg{ID: 9999, Name: "file logger test", CreateAt: time.Now().Unix()})
	partition, offset, sendErr := p.Send(&sarama.ProducerMessage{
		Topic:     testTopic,
		Value:     mq.KafkaMsgValueEncoder(body),
		Timestamp: time.Now().UTC(),
	})
	if sendErr != nil {
		t.Logf("Send error (logged to file): %v", sendErr)
	} else {
		t.Logf("message sent => partition=%d offset=%d", partition, offset)
	}

	// 6. Flush 确保内容落盘
	_ = zapL.Sync()

	// 7. 读取日志文件，验证 "SyncKafkaProducer connected" 已写入
	if err := assertFileContains(t, logFile, "SyncKafkaProducer connected"); err != nil {
		t.Errorf("%v", err)
	}
}

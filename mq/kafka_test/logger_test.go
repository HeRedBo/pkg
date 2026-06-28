package kafka_test

import (
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

// ─────────────────────────────────────────────
// 简单的文件日志实现，用于测试
// ─────────────────────────────────────────────

// fileLogger 基于 zap 的文件日志实现，满足 mq.Logger 接口
type fileLogger struct {
	logger *zap.Logger
}

// newFileLogger 创建写入指定文件的 fileLogger
// logPath: 日志文件路径，自动创建父目录
func newFileLogger(logPath string) (*fileLogger, error) {
	dir := logPath[:strings.LastIndex(logPath, "/")]
	if err := os.MkdirAll(dir, 0766); err != nil {
		return nil, fmt.Errorf("mkdir %s failed: %w", dir, err)
	}

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s failed: %w", logPath, err)
	}

	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		TimeKey:        "time",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
	}

	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(f),
		zapcore.DebugLevel,
	)

	return &fileLogger{
		logger: zap.New(fileCore, zap.AddCaller()),
	}, nil
}

func (fl *fileLogger) Info(msg string, fields ...zap.Field) {
	fl.logger.Info(msg, fields...)
}

func (fl *fileLogger) Warn(msg string, fields ...zap.Field) {
	fl.logger.Warn(msg, fields...)
}

func (fl *fileLogger) Error(msg string, fields ...zap.Field) {
	fl.logger.Error(msg, fields...)
}

func (fl *fileLogger) Debug(msg string, fields ...zap.Field) {
	fl.logger.Debug(msg, fields...)
}

// Sync 刷新日志缓冲区（调用底层 zap.Sync）
func (fl *fileLogger) Sync() error {
	return fl.logger.Sync()
}

// ─────────────────────────────────────────────
// 日志测试常量
// ─────────────────────────────────────────────

const logDir = "/Users/hehongbo/www/GO/go-search/pkg/mq/kafka_test/logs"

// ─────────────────────────────────────────────
// TestMqLoggerFileOutput 创建文件日志 Logger → 写入测试日志 → 验证文件存在且非空 → 清理
// ─────────────────────────────────────────────
func TestMqLoggerFileOutput(t *testing.T) {
	logFile := fmt.Sprintf("%s/test-mq-%s.log", logDir, time.Now().Format("2006-01-02"))
	t.Logf("target log file: %s", logFile)

	fl, err := newFileLogger(logFile)
	if err != nil {
		t.Fatalf("newFileLogger failed: %v", err)
	}
	t.Cleanup(func() {
		_ = fl.Sync()
		// 清理测试日志文件
		os.Remove(logFile)
	})

	// 写入测试日志
	fl.Info("test info message", zap.String("key", "value"))
	fl.Warn("test warn message")
	fl.Error("test error message")
	fl.Debug("test debug message")
	_ = fl.Sync()

	// 验证文件存在且非空
	info, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("log file stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Error("log file is empty, expected content")
	}
	t.Logf("log file size: %d bytes", info.Size())

	// 验证文件内容包含写入的日志
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file failed: %v", err)
	}
	if !strings.Contains(string(content), "test info message") {
		t.Error("log file does not contain 'test info message'")
	}
}

// ─────────────────────────────────────────────
// TestLoggerWithProducer 使用文件日志 Logger 初始化生产者 → 发送消息 → 验证日志文件有内容
// ─────────────────────────────────────────────
func TestLoggerWithProducer(t *testing.T) {
	checkKafkaAvailable(t)

	logFile := fmt.Sprintf("%s/test-producer-%s.log", logDir, time.Now().Format("2006-01-02"))
	t.Logf("target log file: %s", logFile)

	fl, err := newFileLogger(logFile)
	if err != nil {
		t.Fatalf("newFileLogger failed: %v", err)
	}
	t.Cleanup(func() {
		_ = fl.Sync()
		os.Remove(logFile)
	})

	// 使用文件日志初始化生产者
	const name = "test-file-logger-producer"
	err = mq.InitSyncKafkaProducer(name, testHosts, nil, mq.WithLogger(fl))
	if err != nil {
		t.Fatalf("InitSyncKafkaProducer with file logger failed: %v", err)
	}

	p := mq.GetKafkaSyncProducer(name)
	t.Cleanup(func() {
		_ = p.Close()
		delete(mq.KafkaSyncProducers, name)
	})

	// 发送一条消息
	msg := testMsg{ID: 500, Name: "file logger test", CreateAt: time.Now().Unix()}
	body, _ := json.Marshal(msg)
	_, _, sendErr := p.Send(&sarama.ProducerMessage{
		Topic:     "test-sync-topic",
		Value:     mq.KafkaMsgValueEncoder(body),
		Timestamp: time.Now().UTC(),
	})
	if sendErr != nil {
		t.Logf("Send error (logged to file): %v", sendErr)
	}

	// 刷新日志确保落盘
	_ = fl.Sync()

	// 验证日志文件有内容
	info, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("log file stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Error("log file is empty, expected producer log content")
	}
	t.Logf("producer log file size: %d bytes", info.Size())

	// 验证包含关键日志
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file failed: %v", err)
	}
	if !strings.Contains(string(content), "SyncKafkaProducer connected") {
		t.Error("log file does not contain 'SyncKafkaProducer connected'")
	}
}

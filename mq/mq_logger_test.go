package mq

import (
	"bytes"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// ─────────────────────────────────────────────
// 测试辅助：构建一个可观测的 Zap Logger（无文件 I/O，日志写入内存）
// ─────────────────────────────────────────────

// newObservedZap 返回 (*zap.Logger, *observer.ObservedLogs)
// observer.ObservedLogs 可断言具体日志条目
func newObservedZap(lvl zapcore.Level) (*zap.Logger, *observer.ObservedLogs) {
	core, logs := observer.New(lvl)
	return zap.New(core), logs
}

// ─────────────────────────────────────────────
// 辅助：每个测试后重置全局 Logger，避免测试间污染
// ─────────────────────────────────────────────

func resetGlobalLogger(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { globalLogger = nil })
}

// ─────────────────────────────────────────────
// 测试 defaultLogger：无任何注入时应输出到控制台（标准库 log）
// ─────────────────────────────────────────────

// TestDefaultLogger_ConsoleOutput 验证默认 Logger 使用标准库 log 输出到控制台
func TestDefaultLogger_ConsoleOutput(t *testing.T) {
	resetGlobalLogger(t)

	// 替换 stdDefault 的底层 writer 为可捕获的 buffer
	var buf bytes.Buffer
	original := stdDefault.l
	stdDefault.l.SetOutput(&buf)
	t.Cleanup(func() { stdDefault.l = original })

	l := getLogger(nil)
	l.Info("hello from default logger")

	output := buf.String()
	if !strings.Contains(output, "[INFO]") || !strings.Contains(output, "hello from default logger") {
		t.Errorf("defaultLogger output mismatch, got: %q", output)
	}
}

// ─────────────────────────────────────────────
// 测试 SetLogger（全局注入）
// ─────────────────────────────────────────────

// TestSetLogger_GlobalInject 验证全局注入后 getLogger 返回注入的 Logger
func TestSetLogger_GlobalInject(t *testing.T) {
	resetGlobalLogger(t)

	zapLogger, logs := newObservedZap(zapcore.DebugLevel)
	SetLogger(zapLogger)

	l := getLogger(nil) // 无 Option 注入
	l.Warn("global logger warn", zap.String("key", "val"))

	if logs.Len() != 1 {
		t.Fatalf("expected 1 log entry, got %d", logs.Len())
	}
	entry := logs.All()[0]
	if entry.Level != zapcore.WarnLevel {
		t.Errorf("expected Warn level, got %s", entry.Level)
	}
	if entry.Message != "global logger warn" {
		t.Errorf("expected message 'global logger warn', got %q", entry.Message)
	}
}

// ─────────────────────────────────────────────
// 测试 WithLogger Option（优先级最高）
// ─────────────────────────────────────────────

// TestWithLogger_OptionPriority 验证 Option 注入优先于全局 SetLogger
func TestWithLogger_OptionPriority(t *testing.T) {
	resetGlobalLogger(t)

	// 全局设置一个 Logger
	globalZap, globalLogs := newObservedZap(zapcore.DebugLevel)
	SetLogger(globalZap)

	// Option 设置另一个 Logger（优先级更高）
	optionZap, optionLogs := newObservedZap(zapcore.DebugLevel)

	o := applyOptions([]Option{WithLogger(optionZap)})
	l := getLogger(o.logger)
	l.Error("should go to option logger", zap.String("source", "option"))

	// option logger 应收到日志
	if optionLogs.Len() != 1 {
		t.Errorf("option logger: expected 1 log, got %d", optionLogs.Len())
	}
	// global logger 不应收到
	if globalLogs.Len() != 0 {
		t.Errorf("global logger: expected 0 logs, got %d", globalLogs.Len())
	}
}

// ─────────────────────────────────────────────
// 测试优先级完整链：Option > 全局 > 默认控制台
// ─────────────────────────────────────────────

// TestLoggerPriorityChain 验证三级优先级全链路
func TestLoggerPriorityChain(t *testing.T) {
	resetGlobalLogger(t)

	t.Run("no injection => default console logger", func(t *testing.T) {
		l := getLogger(nil)
		// defaultLogger 类型断言
		if _, ok := l.(*defaultLogger); !ok {
			t.Errorf("expected *defaultLogger, got %T", l)
		}
	})

	t.Run("global inject => global logger returned", func(t *testing.T) {
		gZap, _ := newObservedZap(zapcore.DebugLevel)
		SetLogger(gZap)
		t.Cleanup(func() { globalLogger = nil })

		l := getLogger(nil)
		if l != gZap {
			t.Errorf("expected global zap logger, got %T", l)
		}
	})

	t.Run("option inject => option logger overrides global", func(t *testing.T) {
		gZap, _ := newObservedZap(zapcore.DebugLevel)
		SetLogger(gZap)
		t.Cleanup(func() { globalLogger = nil })

		oZap, _ := newObservedZap(zapcore.DebugLevel)
		l := getLogger(oZap) // 直接传 Option Logger
		if l != oZap {
			t.Errorf("expected option zap logger, got %T", l)
		}
	})
}

// ─────────────────────────────────────────────
// 测试 applyOptions
// ─────────────────────────────────────────────

// TestApplyOptions_NilSafe 验证 applyOptions 对 nil Option 安全
func TestApplyOptions_NilSafe(t *testing.T) {
	o := applyOptions(nil)
	if o == nil {
		t.Fatal("applyOptions(nil) should return non-nil *mqOptions")
	}
	if o.logger != nil {
		t.Errorf("expected nil logger, got %v", o.logger)
	}
}

// TestApplyOptions_WithLogger 验证 WithLogger Option 被正确应用
func TestApplyOptions_WithLogger(t *testing.T) {
	zapLogger, _ := newObservedZap(zapcore.InfoLevel)
	o := applyOptions([]Option{WithLogger(zapLogger)})
	if o.logger != zapLogger {
		t.Errorf("expected injected logger, got %T", o.logger)
	}
}

// ─────────────────────────────────────────────
// 测试 logf 辅助函数
// ─────────────────────────────────────────────

// TestLogf_FormatsMessage 验证 logf 将格式化字符串作为 Warn 输出
func TestLogf_FormatsMessage(t *testing.T) {
	zapLogger, logs := newObservedZap(zapcore.DebugLevel)
	logf(zapLogger, "reconnect attempt %d for %s", 3, "my-producer")

	if logs.Len() != 1 {
		t.Fatalf("expected 1 log entry, got %d", logs.Len())
	}
	entry := logs.All()[0]
	expected := "reconnect attempt 3 for my-producer"
	if entry.Message != expected {
		t.Errorf("expected %q, got %q", expected, entry.Message)
	}
	if entry.Level != zapcore.WarnLevel {
		t.Errorf("expected Warn level, got %s", entry.Level)
	}
}

// ─────────────────────────────────────────────
// 测试 saramaZapLogger（sarama.StdLogger 适配器）
// ─────────────────────────────────────────────

// TestSaramaZapLogger_Print 验证 sarama 适配器三个方法均路由到 Debug
func TestSaramaZapLogger_Print(t *testing.T) {
	zapLogger, logs := newObservedZap(zapcore.DebugLevel)
	sl := &saramaZapLogger{l: zapLogger}

	sl.Print("sarama Print message")
	sl.Printf("sarama Printf %s", "formatted")
	sl.Println("sarama Println message")

	if logs.Len() != 3 {
		t.Fatalf("expected 3 log entries, got %d", logs.Len())
	}
	for _, e := range logs.All() {
		if e.Level != zapcore.DebugLevel {
			t.Errorf("expected Debug level, got %s", e.Level)
		}
	}
	if !strings.Contains(logs.All()[0].Message, "sarama Print message") {
		t.Errorf("Print message mismatch: %q", logs.All()[0].Message)
	}
	if !strings.Contains(logs.All()[1].Message, "sarama Printf formatted") {
		t.Errorf("Printf message mismatch: %q", logs.All()[1].Message)
	}
}

// TestSetSaramaLogger_NilRestores 验证传 nil 时不 panic
func TestSetSaramaLogger_NilRestores(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("SetSaramaLogger(nil) should not panic, got: %v", r)
		}
	}()
	SetSaramaLogger(nil)
}

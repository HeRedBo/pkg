package mq

import (
	"errors"
	"os"
	"path/filepath"
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

// wrapObservedZap 返回可观测的 mq.Logger 适配器及其底层 *observer.ObservedLogs
func wrapObservedZap(lvl zapcore.Level) (Logger, *observer.ObservedLogs) {
	zapLogger, logs := newObservedZap(lvl)
	return wrapZapLogger(zapLogger), logs
}

// ─────────────────────────────────────────────
// 辅助：每个测试后重置全局 Logger，避免测试间污染
// ─────────────────────────────────────────────

func resetGlobalLogger(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { globalLogger = nil })
}

// ─────────────────────────────────────────────
// 测试 defaultLogger：使用 *zap.Logger，保留结构化字段
// ─────────────────────────────────────────────

// TestDefaultLogger_StructuredFields 验证默认 Logger 使用 zap 输出，保留结构化字段
func TestDefaultLogger_StructuredFields(t *testing.T) {
	resetGlobalLogger(t)

	// 用 observer 替换 stdDefault 的内部 zap.Logger，以捕获输出
	origDefault := stdDefault
	core, logs := observer.New(zapcore.DebugLevel)
	stdDefault = &defaultLogger{l: zap.New(core)}
	t.Cleanup(func() { stdDefault = origDefault })

	l := getLogger(nil)
	l.Info("hello from default logger", Field("key", "val"))

	if logs.Len() != 1 {
		t.Fatalf("expected 1 log entry, got %d", logs.Len())
	}
	entry := logs.All()[0]
	if entry.Message != "hello from default logger" {
		t.Errorf("message mismatch, got: %q", entry.Message)
	}
	if entry.Level != zapcore.InfoLevel {
		t.Errorf("expected Info level, got %s", entry.Level)
	}
	// 验证结构化字段被保留
	ctx := entry.ContextMap()
	if v, ok := ctx["key"]; !ok || v != "val" {
		t.Errorf("expected field key=val, got context: %v", ctx)
	}
}

// TestDefaultLogger_TypeAssertion 验证无注入时 getLogger 返回 *defaultLogger
func TestDefaultLogger_TypeAssertion(t *testing.T) {
	resetGlobalLogger(t)

	l := getLogger(nil)
	if _, ok := l.(*defaultLogger); !ok {
		t.Errorf("expected *defaultLogger, got %T", l)
	}
}

// ─────────────────────────────────────────────
// 测试 SetLogger（全局注入）
// ─────────────────────────────────────────────

// TestSetLogger_GlobalInject 验证全局注入后 getLogger 返回注入的 Logger
func TestSetLogger_GlobalInject(t *testing.T) {
	resetGlobalLogger(t)

	injected, logs := wrapObservedZap(zapcore.DebugLevel)
	SetLogger(injected)

	l := getLogger(nil) // 无 Option 注入
	l.Warn("global logger warn", Field("key", "val"))

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
	globalLoggerAdapter, globalLogs := wrapObservedZap(zapcore.DebugLevel)
	SetLogger(globalLoggerAdapter)

	// Option 设置另一个 Logger（优先级更高）
	optionLogger, optionLogs := wrapObservedZap(zapcore.DebugLevel)

	o := applyOptions([]Option{WithLogger(optionLogger)})
	l := getLogger(o.logger)
	l.Error("should go to option logger", Field("source", "option"))

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
		gAdapter, _ := wrapObservedZap(zapcore.DebugLevel)
		SetLogger(gAdapter)
		t.Cleanup(func() { globalLogger = nil })

		l := getLogger(nil)
		if l != gAdapter {
			t.Errorf("expected global logger, got %T", l)
		}
	})

	t.Run("option inject => option logger overrides global", func(t *testing.T) {
		gAdapter, _ := wrapObservedZap(zapcore.DebugLevel)
		SetLogger(gAdapter)
		t.Cleanup(func() { globalLogger = nil })

		oAdapter, _ := wrapObservedZap(zapcore.DebugLevel)
		l := getLogger(oAdapter) // 直接传 Option Logger
		if l != oAdapter {
			t.Errorf("expected option logger, got %T", l)
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
	adapter, _ := wrapObservedZap(zapcore.InfoLevel)
	o := applyOptions([]Option{WithLogger(adapter)})
	if o.logger != adapter {
		t.Errorf("expected injected logger, got %T", o.logger)
	}
}

// ─────────────────────────────────────────────
// 测试 saramaLogger（sarama.StdLogger 适配器）
// ─────────────────────────────────────────────

// TestSaramaLogger_Print 验证 sarama 适配器三个方法均路由到 Debug
func TestSaramaLogger_Print(t *testing.T) {
	zapLogger, logs := newObservedZap(zapcore.DebugLevel)
	adapter := wrapZapLogger(zapLogger)
	sl := &saramaLogger{l: adapter}

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

// ─────────────────────────────────────────────
// 新增：Field / ErrField 辅助函数测试
// ─────────────────────────────────────────────

// TestField_Helper 验证 Field 辅助函数创建正确的 LogField
func TestField_Helper(t *testing.T) {
	f := Field("key", "val")
	if f.Key != "key" {
		t.Errorf("expected key='key', got %q", f.Key)
	}
	if f.Value != "val" {
		t.Errorf("expected value='val', got %v", f.Value)
	}

	// 测试不同类型的值
	f2 := Field("count", 42)
	if f2.Key != "count" || f2.Value != 42 {
		t.Errorf("Field with int mismatch: %+v", f2)
	}
}

// TestErrField_Helper 验证 ErrField 辅助函数创建正确的 LogField
func TestErrField_Helper(t *testing.T) {
	testErr := errors.New("test error")
	f := ErrField(testErr)
	if f.Key != "error" {
		t.Errorf("expected key='error', got %q", f.Key)
	}
	if f.Value != testErr {
		t.Errorf("expected value=testErr, got %v", f.Value)
	}

	// nil error 也应正常工作
	f2 := ErrField(nil)
	if f2.Key != "error" {
		t.Errorf("expected key='error' for nil error, got %q", f2.Key)
	}
}

// ─────────────────────────────────────────────
// 新增：WithFields 测试
// ─────────────────────────────────────────────

// TestWithFields_DefaultLogger 验证 defaultLogger 的 WithFields 预绑定
func TestWithFields_DefaultLogger(t *testing.T) {
	resetGlobalLogger(t)

	origDefault := stdDefault
	core, logs := observer.New(zapcore.DebugLevel)
	stdDefault = &defaultLogger{l: zap.New(core)}
	t.Cleanup(func() { stdDefault = origDefault })

	// 预绑定字段
	prefixed := getLogger(nil).WithFields(Field("module", "test"))
	prefixed.Info("with fields message", Field("extra", "data"))

	if logs.Len() != 1 {
		t.Fatalf("expected 1 log entry, got %d", logs.Len())
	}
	ctx := logs.All()[0].ContextMap()
	if v, ok := ctx["module"]; !ok || v != "test" {
		t.Errorf("expected prefetched field module=test, got context: %v", ctx)
	}
	if v, ok := ctx["extra"]; !ok || v != "data" {
		t.Errorf("expected call-time field extra=data, got context: %v", ctx)
	}
}

// TestWithFields_Chained 验证链式 WithFields 字段累加
func TestWithFields_Chained(t *testing.T) {
	resetGlobalLogger(t)

	origDefault := stdDefault
	core, logs := observer.New(zapcore.DebugLevel)
	stdDefault = &defaultLogger{l: zap.New(core)}
	t.Cleanup(func() { stdDefault = origDefault })

	// 链式 WithFields
	l := getLogger(nil).
		WithFields(Field("a", "1")).
		WithFields(Field("b", "2"))
	l.Info("chained fields")

	if logs.Len() != 1 {
		t.Fatalf("expected 1 log entry, got %d", logs.Len())
	}
	ctx := logs.All()[0].ContextMap()
	if v, ok := ctx["a"]; !ok || v != "1" {
		t.Errorf("expected field a=1, got context: %v", ctx)
	}
	if v, ok := ctx["b"]; !ok || v != "2" {
		t.Errorf("expected field b=2, got context: %v", ctx)
	}
}

// ─────────────────────────────────────────────
// 新增：InitLogger 测试
// ─────────────────────────────────────────────

// TestInitLogger_Console 验证 console 模式返回可用 Logger
func TestInitLogger_Console(t *testing.T) {
	l, err := InitLogger(&LogConfig{Mode: "console", Level: "debug"})
	if err != nil {
		t.Fatalf("InitLogger console mode failed: %v", err)
	}
	if l == nil {
		t.Fatal("expected non-nil Logger")
	}
	// Logger 接口使用 LogField
	l.Info("console test", Field("mode", "console"))
}

// TestInitLogger_File 验证 file 模式写入指定文件
func TestInitLogger_File(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	l, err := InitLogger(&LogConfig{Mode: "file", Path: logPath, Level: "info"})
	if err != nil {
		t.Fatalf("InitLogger file mode failed: %v", err)
	}
	l.Info("file log message", Field("key", "value"))

	// 同步确保写入（通过类型断言获取底层 zap.Logger）
	if wl, ok := l.(*zapLoggerWrapper); ok {
		_ = wl.l.Sync()
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "file log message") {
		t.Errorf("log file should contain 'file log message', got: %q", content)
	}
	if !strings.Contains(content, "key") {
		t.Errorf("log file should contain field 'key', got: %q", content)
	}
}

// TestInitLogger_FileWithRotation 验证 file + Rotation=true 模式使用 lumberjack
func TestInitLogger_FileWithRotation(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "rotation.log")

	l, err := InitLogger(&LogConfig{
		Mode:       "file",
		Path:       logPath,
		Level:      "info",
		Rotation:   true,
		MaxSize:    10,
		MaxAge:     7,
		MaxBackups: 3,
		Compress:   false,
	})
	if err != nil {
		t.Fatalf("InitLogger rotation mode failed: %v", err)
	}
	l.Info("rotation log message", Field("env", "test"))

	if wl, ok := l.(*zapLoggerWrapper); ok {
		_ = wl.l.Sync()
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read rotation log file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "rotation log message") {
		t.Errorf("rotation log file should contain 'rotation log message', got: %q", content)
	}
}

// TestInitLogger_NilConfig 验证 nil 配置使用默认值（console + info）
func TestInitLogger_NilConfig(t *testing.T) {
	l, err := InitLogger(nil)
	if err != nil {
		t.Fatalf("InitLogger nil config failed: %v", err)
	}
	if l == nil {
		t.Fatal("expected non-nil Logger")
	}
}

// TestInitLogger_Levels 验证各种 Level 设置
func TestInitLogger_Levels(t *testing.T) {
	levels := []string{"debug", "info", "warn", "error", ""}
	for _, lvl := range levels {
		t.Run("level="+lvl, func(t *testing.T) {
			l, err := InitLogger(&LogConfig{Mode: "console", Level: lvl})
			if err != nil {
				t.Fatalf("InitLogger with level %q failed: %v", lvl, err)
			}
			if l == nil {
				t.Fatal("expected non-nil Logger")
			}
		})
	}
}

// ─────────────────────────────────────────────
// 新增：ResetLogger 测试
// ─────────────────────────────────────────────

// TestResetLogger 验证 ResetLogger 后回退到默认 logger
func TestResetLogger(t *testing.T) {
	// 先注入全局 logger
	adapter, _ := wrapObservedZap(zapcore.DebugLevel)
	SetLogger(adapter)

	l := getLogger(nil)
	if l != adapter {
		t.Errorf("expected global logger, got %T", l)
	}

	// 重置后应回退到默认
	ResetLogger()

	l = getLogger(nil)
	if _, ok := l.(*defaultLogger); !ok {
		t.Errorf("after ResetLogger, expected *defaultLogger, got %T", l)
	}
}

// TestResetLogger_Idempotent 验证多次 ResetLogger 不 panic
func TestResetLogger_Idempotent(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("multiple ResetLogger calls should not panic, got: %v", r)
		}
	}()
	ResetLogger()
	ResetLogger()
	ResetLogger()
}

// ─────────────────────────────────────────────
// 测试 getLogger 不再触发 SetSaramaLogger 副作用
// ─────────────────────────────────────────────

// TestGetLogger_NoSaramaSideEffect 验证 getLogger 不再自动设置 sarama.Logger
func TestGetLogger_NoSaramaSideEffect(t *testing.T) {
	resetGlobalLogger(t)

	// getLogger 调用不应影响 sarama.Logger
	_ = getLogger(nil)
	// 此处仅验证不 panic，sarama.Logger 的状态由 SetSaramaLogger 显式管理
}

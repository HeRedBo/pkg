package mq

import (
	"errors"
	"strings"
	"testing"

	"github.com/HeRedBo/pkg/logx"
	"github.com/HeRedBo/pkg/logx/zapx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func newObservedZap(lvl zapcore.Level) (*zap.Logger, *observer.ObservedLogs) {
	core, logs := observer.New(lvl)
	return zap.New(core), logs
}

func wrapObservedZap(lvl zapcore.Level) (Logger, *observer.ObservedLogs) {
	zapLogger, logs := newObservedZap(lvl)
	return zapx.New(zapLogger), logs
}

func resetAll(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		logx.ResetLogger()
		SetSaramaLogger(nil)
	})
}

func TestSetLogger_GlobalInject(t *testing.T) {
	resetAll(t)

	injected, logs := wrapObservedZap(zapcore.DebugLevel)
	SetLogger(injected)

	l := getLogger(nil)
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

func TestSetLogger_SaramaBridge(t *testing.T) {
	resetAll(t)

	injected, logs := wrapObservedZap(zapcore.DebugLevel)
	SetLogger(injected)

	sl := &saramaLogger{l: getLogger(nil)}
	sl.Print("sarama via SetLogger")

	if logs.Len() != 1 {
		t.Fatalf("expected 1 log entry from sarama bridge, got %d", logs.Len())
	}
	if !strings.Contains(logs.All()[0].Message, "sarama via SetLogger") {
		t.Errorf("sarama bridge message mismatch: %q", logs.All()[0].Message)
	}
}

func TestWithLogger_OptionPriority(t *testing.T) {
	resetAll(t)

	globalLoggerAdapter, globalLogs := wrapObservedZap(zapcore.DebugLevel)
	SetLogger(globalLoggerAdapter)

	optionLogger, optionLogs := wrapObservedZap(zapcore.DebugLevel)

	o := applyOptions([]Option{WithLogger(optionLogger)})
	l := getLogger(o.logger)
	l.Error("should go to option logger", Field("source", "option"))

	if optionLogs.Len() != 1 {
		t.Errorf("option logger: expected 1 log, got %d", optionLogs.Len())
	}
	if globalLogs.Len() != 0 {
		t.Errorf("global logger: expected 0 logs, got %d", globalLogs.Len())
	}
}

func TestLoggerPriorityChain(t *testing.T) {
	resetAll(t)

	t.Run("no injection => logx default logger", func(t *testing.T) {
		l := getLogger(nil)
		if l == nil {
			t.Error("expected non-nil Logger from getLogger(nil)")
		}
	})

	t.Run("global inject => global logger returned", func(t *testing.T) {
		gAdapter, _ := wrapObservedZap(zapcore.DebugLevel)
		SetLogger(gAdapter)

		l := getLogger(nil)
		if l != gAdapter {
			t.Errorf("expected global logger, got %T", l)
		}
	})

	t.Run("option inject => option logger overrides global", func(t *testing.T) {
		gAdapter, _ := wrapObservedZap(zapcore.DebugLevel)
		SetLogger(gAdapter)

		oAdapter, _ := wrapObservedZap(zapcore.DebugLevel)
		l := getLogger(oAdapter)
		if l != oAdapter {
			t.Errorf("expected option logger, got %T", l)
		}
	})
}

func TestApplyOptions_NilSafe(t *testing.T) {
	o := applyOptions(nil)
	if o == nil {
		t.Fatal("applyOptions(nil) should return non-nil *mqOptions")
	}
	if o.logger != nil {
		t.Errorf("expected nil logger, got %v", o.logger)
	}
}

func TestApplyOptions_WithLogger(t *testing.T) {
	adapter, _ := wrapObservedZap(zapcore.InfoLevel)
	o := applyOptions([]Option{WithLogger(adapter)})
	if o.logger != adapter {
		t.Errorf("expected injected logger, got %T", o.logger)
	}
}

func TestSaramaLogger_Print(t *testing.T) {
	zapLogger, logs := newObservedZap(zapcore.DebugLevel)
	adapter := zapx.New(zapLogger)
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

func TestSetSaramaLogger_NilRestores(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("SetSaramaLogger(nil) should not panic, got: %v", r)
		}
	}()
	SetSaramaLogger(nil)
}

func TestField_Helper(t *testing.T) {
	f := Field("key", "val")
	if f.Key != "key" {
		t.Errorf("expected key='key', got %q", f.Key)
	}
	if f.Value != "val" {
		t.Errorf("expected value='val', got %v", f.Value)
	}

	f2 := Field("count", 42)
	if f2.Key != "count" || f2.Value != 42 {
		t.Errorf("Field with int mismatch: %+v", f2)
	}
}

func TestErrField_Helper(t *testing.T) {
	testErr := errors.New("test error")
	f := ErrField(testErr)
	if f.Key != "error" {
		t.Errorf("expected key='error', got %q", f.Key)
	}
	if f.Value != testErr {
		t.Errorf("expected value=testErr, got %v", f.Value)
	}

	f2 := ErrField(nil)
	if f2.Key != "error" {
		t.Errorf("expected key='error' for nil error, got %q", f2.Key)
	}
}

func TestWithFields_ViaSetLogger(t *testing.T) {
	resetAll(t)

	obsLogger, logs := wrapObservedZap(zapcore.DebugLevel)
	SetLogger(obsLogger)

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

func TestWithFields_Chained(t *testing.T) {
	resetAll(t)

	obsLogger, logs := wrapObservedZap(zapcore.DebugLevel)
	SetLogger(obsLogger)

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

// --- InitLogger ---
// InitLogger delegates to logx.InitLogger, so we test the API contract
// (returns non-nil Logger, no error) rather than file content verification.
// File content correctness is tested in the logx package.

func TestInitLogger_Console(t *testing.T) {
	l, err := InitLogger(&LogConfig{Mode: "console", Level: "debug"})
	if err != nil {
		t.Fatalf("InitLogger console mode failed: %v", err)
	}
	if l == nil {
		t.Fatal("expected non-nil Logger")
	}
	l.Info("console test", Field("mode", "console"))
}

func TestInitLogger_File(t *testing.T) {
	l, err := InitLogger(&LogConfig{Mode: "file", Path: "test_output.log", Level: "info"})
	if err != nil {
		t.Fatalf("InitLogger file mode failed: %v", err)
	}
	if l == nil {
		t.Fatal("expected non-nil Logger")
	}
	l.Info("file log message", Field("key", "value"))
}

func TestInitLogger_FileWithRotation(t *testing.T) {
	l, err := InitLogger(&LogConfig{
		Mode:       "file",
		Path:       "test_rotation.log",
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
	if l == nil {
		t.Fatal("expected non-nil Logger")
	}
	l.Info("rotation log message", Field("env", "test"))
}

func TestInitLogger_NilConfig(t *testing.T) {
	l, err := InitLogger(nil)
	if err != nil {
		t.Fatalf("InitLogger nil config failed: %v", err)
	}
	if l == nil {
		t.Fatal("expected non-nil Logger")
	}
}

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

// --- ResetLogger ---

func TestResetLogger(t *testing.T) {
	adapter, _ := wrapObservedZap(zapcore.DebugLevel)
	SetLogger(adapter)

	l := getLogger(nil)
	if l != adapter {
		t.Errorf("expected global logger, got %T", l)
	}

	ResetLogger()

	l = getLogger(nil)
	if l == nil {
		t.Error("after ResetLogger, expected non-nil default Logger")
	}
}

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

func TestGetLogger_NoSaramaSideEffect(t *testing.T) {
	resetAll(t)
	_ = getLogger(nil)
}

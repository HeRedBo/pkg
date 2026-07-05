package mq

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// ─────────────────────────────────────────────
// zapLoggerWrapper 内部使用，包装 *zap.Logger 实现 Logger 接口
// ─────────────────────────────────────────────

type zapLoggerWrapper struct {
	l      *zap.Logger
	fields []zap.Field
}

func (z *zapLoggerWrapper) Info(msg string, fields ...LogField) {
	z.l.Info(msg, append(z.fields, toZapFields(fields)...)...)
}
func (z *zapLoggerWrapper) Warn(msg string, fields ...LogField) {
	z.l.Warn(msg, append(z.fields, toZapFields(fields)...)...)
}
func (z *zapLoggerWrapper) Error(msg string, fields ...LogField) {
	z.l.Error(msg, append(z.fields, toZapFields(fields)...)...)
}
func (z *zapLoggerWrapper) Debug(msg string, fields ...LogField) {
	z.l.Debug(msg, append(z.fields, toZapFields(fields)...)...)
}
func (z *zapLoggerWrapper) WithFields(fields ...LogField) Logger {
	merged := make([]zap.Field, len(z.fields)+len(fields))
	copy(merged, z.fields)
	copy(merged[len(z.fields):], toZapFields(fields))
	return &zapLoggerWrapper{l: z.l, fields: merged}
}

// wrapZapLogger 将 *zap.Logger 包装为 Logger 接口
func wrapZapLogger(l *zap.Logger) Logger {
	return &zapLoggerWrapper{l: l}
}

// ─────────────────────────────────────────────
// LogConfig 日志配置，参考 go-zero logx.LogConf 设计
// ─────────────────────────────────────────────

// LogConfig 日志配置
// Mode 为空时默认 "console"，Level 为空时默认 "info"
type LogConfig struct {
	Mode       string // 日志模式: "console" (默认) 或 "file"
	Path       string // 日志文件路径（Mode=file 时使用）
	Level      string // 日志级别: "debug", "info", "warn", "error" (默认 "info")
	Compress   bool   // 是否压缩日志文件
	KeepDays   int    // 日志保留天数
	MaxSize    int    // 单个日志文件最大 MB，默认 100
	MaxAge     int    // 日志保留最大天数，默认 30
	MaxBackups int    // 保留日志文件最大数量，默认 5
	Rotation   bool   // 是否启用日志轮转（Mode=file 时生效）
}

// InitLogger 根据配置初始化 Logger
//   - Mode=console: zap development 格式输出到控制台
//   - Mode=file + Rotation=false: zap production JSON 格式输出到指定文件
//   - Mode=file + Rotation=true: 使用 lumberjack 做日志轮转 + zap production JSON encoder
func InitLogger(cfg *LogConfig) (Logger, error) {
	if cfg == nil {
		cfg = &LogConfig{Mode: "console", Level: "info"}
	}

	mode := cfg.Mode
	if mode == "" {
		mode = "console"
	}

	level := parseLevel(cfg.Level)

	switch mode {
	case "file":
		if cfg.Rotation {
			return buildRotationLogger(cfg, level)
		}
		return buildFileLogger(cfg, level)
	default:
		return buildConsoleLogger(level)
	}
}

// parseLevel 将字符串级别映射为 zap.AtomicLevel，默认 info
func parseLevel(lvl string) zap.AtomicLevel {
	switch lvl {
	case "debug":
		return zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case "warn":
		return zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case "error":
		return zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	default:
		return zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}
}

// buildConsoleLogger console 模式：zap development 格式输出到 stdout
func buildConsoleLogger(level zap.AtomicLevel) (Logger, error) {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = level
	cfg.OutputPaths = []string{"stdout"}
	l, err := cfg.Build()
	if err != nil {
		return nil, err
	}
	return wrapZapLogger(l), nil
}

// buildFileLogger file 模式 + Rotation=false：production JSON 格式输出到指定文件
func buildFileLogger(cfg *LogConfig, level zap.AtomicLevel) (Logger, error) {
	zcfg := zap.NewProductionConfig()
	zcfg.Level = level
	zcfg.OutputPaths = []string{cfg.Path}
	l, err := zcfg.Build()
	if err != nil {
		return nil, err
	}
	return wrapZapLogger(l), nil
}

// buildRotationLogger file 模式 + Rotation=true：lumberjack 日志轮转 + zap production encoder
func buildRotationLogger(cfg *LogConfig, level zap.AtomicLevel) (Logger, error) {
	maxSize := cfg.MaxSize
	if maxSize == 0 {
		maxSize = 100
	}
	maxAge := cfg.MaxAge
	if maxAge == 0 {
		maxAge = 30
	}
	maxBackups := cfg.MaxBackups
	if maxBackups == 0 {
		maxBackups = 5
	}

	w := &lumberjack.Logger{
		Filename:   cfg.Path,
		MaxSize:    maxSize,
		MaxAge:     maxAge,
		MaxBackups: maxBackups,
		Compress:   cfg.Compress,
		LocalTime:  true,
	}

	if cfg.KeepDays > 0 {
		w.MaxAge = cfg.KeepDays
	}

	// 使用 production encoder (JSON)
	zcfg := zap.NewProductionConfig()
	encoder := zapcore.NewJSONEncoder(zcfg.EncoderConfig)

	core := zapcore.NewCore(encoder, zapcore.AddSync(w), level)
	l := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	return wrapZapLogger(l), nil
}

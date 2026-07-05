package mq

import (
	"go.uber.org/zap"
)

// ─────────────────────────────────────────────
// LogField 框架自定义的日志字段，不绑定任何第三方日志库
// ─────────────────────────────────────────────

// LogField 框架无关的日志字段
type LogField struct {
	Key   string
	Value interface{}
}

// Field 创建日志字段
func Field(key string, val interface{}) LogField {
	return LogField{Key: key, Value: val}
}

// ErrField 创建错误日志字段
func ErrField(err error) LogField {
	return LogField{Key: "error", Value: err}
}

// ─────────────────────────────────────────────
// Logger 接口：mq 包对外暴露的日志抽象
// 使用自定义 LogField，不绑定任何第三方日志库
// ─────────────────────────────────────────────

// Logger mq 包使用的日志接口
type Logger interface {
	Info(msg string, fields ...LogField)
	Warn(msg string, fields ...LogField)
	Error(msg string, fields ...LogField)
	Debug(msg string, fields ...LogField)
	WithFields(fields ...LogField) Logger
}

// ─────────────────────────────────────────────
// 全局默认 Logger（控制台输出，兼容旧行为）
// ─────────────────────────────────────────────

// globalLogger 全局注入的 Logger，nil 时回退到 defaultLogger
var globalLogger Logger

// defaultLogger 默认日志实现：包装 *zap.Logger，保留结构化字段
// 仅在未注入任何 Logger 时使用，保证零配置可运行
type defaultLogger struct {
	l      *zap.Logger
	fields []zap.Field // 预绑定的 zap 字段（用于 WithFields）
}

func newDefaultLogger() *defaultLogger {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{"stdout"}
	l, _ := cfg.Build()
	return &defaultLogger{l: l}
}

func (d *defaultLogger) Info(msg string, fields ...LogField) {
	d.l.Info(msg, append(d.fields, toZapFields(fields)...)...)
}
func (d *defaultLogger) Warn(msg string, fields ...LogField) {
	d.l.Warn(msg, append(d.fields, toZapFields(fields)...)...)
}
func (d *defaultLogger) Error(msg string, fields ...LogField) {
	d.l.Error(msg, append(d.fields, toZapFields(fields)...)...)
}
func (d *defaultLogger) Debug(msg string, fields ...LogField) {
	d.l.Debug(msg, append(d.fields, toZapFields(fields)...)...)
}
func (d *defaultLogger) WithFields(fields ...LogField) Logger {
	merged := make([]zap.Field, len(d.fields)+len(fields))
	copy(merged, d.fields)
	copy(merged[len(d.fields):], toZapFields(fields))
	return &defaultLogger{l: d.l, fields: merged}
}

// toZapFields 内部转换函数：将 LogField 转换为 zap.Field
func toZapFields(fields []LogField) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	zf := make([]zap.Field, len(fields))
	for i, f := range fields {
		zf[i] = zap.Any(f.Key, f.Value)
	}
	return zf
}

// stdDefault 内置默认实例
var stdDefault = newDefaultLogger()

// getLogger 获取当前生效的 Logger
// 优先级：Option 注入 > 全局 SetLogger > 默认控制台
func getLogger(opt Logger) Logger {
	switch {
	case opt != nil:
		return opt
	case globalLogger != nil:
		return globalLogger
	default:
		return stdDefault
	}
}

// ResetLogger 重置全局 Logger，使后续调用回退到默认实现
func ResetLogger() { globalLogger = nil }

// SetLogger 全局注入 Logger（如 *zap.Logger）
// 适用于整个应用统一使用同一 Logger 的场景
// 调用时机：业务项目 init() 或 main() 初始化阶段，在 InitSyncKafkaProducer 之前
func SetLogger(l Logger) {
	globalLogger = l
}

// ─────────────────────────────────────────────
// Functional Options
// ─────────────────────────────────────────────

// mqOptions 初始化选项集合
type mqOptions struct {
	logger Logger
}

// Option 初始化选项函数
type Option func(*mqOptions)

// WithLogger 通过 Option 注入 Logger（优先级最高）
// 适用于同一应用内不同 Producer/Consumer 需要使用不同 Logger 的场景
func WithLogger(l Logger) Option {
	return func(o *mqOptions) {
		o.logger = l
	}
}

// applyOptions 合并所有 Options
func applyOptions(opts []Option) *mqOptions {
	o := &mqOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return o
}

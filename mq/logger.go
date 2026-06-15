package mq

import (
	"fmt"
	"log"
	"os"

	"go.uber.org/zap"
)

// ─────────────────────────────────────────────
// Logger 接口：mq 包对外暴露的日志抽象
// *zap.Logger 天然满足此接口，无需额外适配器
// ─────────────────────────────────────────────

// Logger mq 包使用的日志接口，*zap.Logger 直接满足
type Logger interface {
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
}

// ─────────────────────────────────────────────
// 全局默认 Logger（控制台输出，兼容旧行为）
// ─────────────────────────────────────────────

// globalLogger 全局注入的 Logger，nil 时回退到 defaultLogger
var globalLogger Logger

// defaultLogger 默认日志实现：包装标准库 log，输出到控制台
// 仅在未注入任何 Logger 时使用，保证零配置可运行
type defaultLogger struct {
	l *log.Logger
}

func (d *defaultLogger) Info(msg string, _ ...zap.Field) {
	d.l.Printf("[INFO]  %s", msg)
}

func (d *defaultLogger) Warn(msg string, _ ...zap.Field) {
	d.l.Printf("[WARN]  %s", msg)
}

func (d *defaultLogger) Error(msg string, _ ...zap.Field) {
	d.l.Printf("[ERROR] %s", msg)
}

func (d *defaultLogger) Debug(msg string, _ ...zap.Field) {
	d.l.Printf("[DEBUG] %s", msg)
}

// stdDefault 内置默认实例，输出到 Stdout
var stdDefault = &defaultLogger{
	l: log.New(os.Stdout, "[kafka] ", log.LstdFlags|log.Lshortfile),
}

// getLogger 获取当前生效的 Logger
// 优先级：Option 注入 > 全局 SetLogger > 默认控制台
func getLogger(opt Logger) Logger {
	if opt != nil {
		return opt
	}
	if globalLogger != nil {
		return globalLogger
	}
	return stdDefault
}

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

// logf 兼容旧 stdLogger.Printf 风格的 Warn 输出，用于 keepConnect / check 等处
func logf(l Logger, format string, args ...interface{}) {
	l.Warn(fmt.Sprintf(format, args...))
}

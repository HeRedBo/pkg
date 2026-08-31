package mq

import (
	"github.com/HeRedBo/pkg/logx"
)

// LogField 框架无关的日志字段（类型别名，实际由 logx 定义）
type LogField = logx.LogField

// Logger mq 包使用的日志接口（类型别名，实际由 logx 定义）
type Logger = logx.Logger

// Field 创建日志字段，委托到 logx.Field
func Field(key string, val interface{}) LogField {
	return logx.Field(key, val)
}

// ErrField 创建错误日志字段，委托到 logx.ErrField
func ErrField(err error) LogField {
	return logx.ErrField(err)
}

// SetLogger 全局注入 Logger
// 同时设置 logx 的全局 Logger 和 Sarama 日志桥接，保持日志状态一致
func SetLogger(l Logger) {
	logx.SetLogger(l)
	SetSaramaLogger(l)
}

// ResetLogger 重置全局 Logger，使后续调用回退到默认实现
// 同时重置 logx 的全局 Logger 和 Sarama 日志桥接
func ResetLogger() {
	logx.ResetLogger()
	SetSaramaLogger(nil)
}

// getLogger 获取当前生效的 Logger
// 优先级：Option 注入 > 全局 logx.GetLogger()
func getLogger(opt Logger) Logger {
	if opt != nil {
		return opt
	}
	return logx.GetLogger()
}

// mqOptions 初始化选项集合
type mqOptions struct {
	logger Logger
}

// Option 初始化选项函数
type Option func(*mqOptions)

// WithLogger 通过 Option 注入 Logger（优先级最高）
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

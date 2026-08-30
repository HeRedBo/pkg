package logx

import "sync"

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
// Logger 接口：logx 包对外暴露的日志抽象
// 使用自定义 LogField，不绑定任何第三方日志库
// ─────────────────────────────────────────────

// Logger 公共日志接口
type Logger interface {
	Info(msg string, fields ...LogField)
	Warn(msg string, fields ...LogField)
	Error(msg string, fields ...LogField)
	Debug(msg string, fields ...LogField)
	WithFields(fields ...LogField) Logger
}

// ─────────────────────────────────────────────
// 全局注入机制（三级优先级）
// ─────────────────────────────────────────────

var (
	mu           sync.RWMutex
	globalLogger Logger
)

// SetLogger 全局注入 Logger
// 适用于整个应用统一使用同一 Logger 的场景
// 调用时机：业务项目 init() 或 main() 初始化阶段
func SetLogger(l Logger) {
	mu.Lock()
	defer mu.Unlock()
	globalLogger = l
}

// GetLogger 获取当前全局 Logger
// 若未注入则返回内置默认实现
func GetLogger() Logger {
	mu.RLock()
	l := globalLogger
	mu.RUnlock()
	if l != nil {
		return l
	}
	return stdDefault
}

// ResetLogger 重置全局 Logger，使后续调用回退到默认实现
func ResetLogger() {
	mu.Lock()
	defer mu.Unlock()
	globalLogger = nil
}

// getLogger 获取当前生效的 Logger
// 优先级：Option 注入 > 全局 SetLogger > 默认控制台
func getLogger(opt Logger) Logger {
	switch {
	case opt != nil:
		return opt
	default:
		return GetLogger()
	}
}

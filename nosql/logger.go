package nosql

import "github.com/HeRedBo/pkg/logx"

// Logger nosql 包使用的日志接口（类型别名，实际由 logx 定义）
type Logger = logx.Logger

// LogField 框架无关的日志字段（类型别名，实际由 logx 定义）
type LogField = logx.LogField

// Field 创建日志字段，委托到 logx.Field
func Field(key string, val interface{}) LogField {
	return logx.Field(key, val)
}

// ErrField 创建错误日志字段，委托到 logx.ErrField
func ErrField(err error) LogField {
	return logx.ErrField(err)
}

// getLogger 获取当前生效的 Logger
// 优先级：Option 注入 > 全局 logx.GetLogger()
func getLogger(opt Logger) Logger {
	if opt != nil {
		return opt
	}
	return logx.GetLogger()
}

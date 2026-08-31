package es

import (
	"fmt"

	"github.com/HeRedBo/pkg/logx"
)

// Logger es 包使用的日志接口（类型别名，实际由 logx 定义）
type Logger = logx.Logger

// LogField 框架无关的日志字段（类型别名，实际由 logx 定义）
type LogField = logx.LogField

// Field 创建日志字段，委托到 logx.Field
var Field = logx.Field

// ErrField 创建错误日志字段，委托到 logx.ErrField
var ErrField = logx.ErrField

// getLogger 获取当前生效的 Logger
// 优先级：Option 注入 > 全局 logx.GetLogger() > 默认控制台
func getLogger(opt Logger) Logger {
	if opt != nil {
		return opt
	}
	return logx.GetLogger()
}

// ─────────────────────────────────────────────
// elasticLogger：将 elastic.StdLogger 桥接到 logx.Logger
//
// elastic SDK 内部（连接管理、健康检查、嗅探等）通过 StdLogger 接口输出日志，
// 通过此适配器，elastic 的底层日志统一走 logx.Logger → 日志后端
// ─────────────────────────────────────────────

// elasticLogger 实现 elastic.StdLogger 接口，内部代理到 logx.Logger
type elasticLogger struct {
	l Logger
}

// Print 实现 elastic.StdLogger
func (e *elasticLogger) Print(v ...interface{}) {
	e.l.Debug(fmt.Sprint(v...))
}

// Printf 实现 elastic.StdLogger
func (e *elasticLogger) Printf(format string, v ...interface{}) {
	e.l.Debug(fmt.Sprintf(format, v...))
}

// Println 实现 elastic.StdLogger
func (e *elasticLogger) Println(v ...interface{}) {
	e.l.Debug(fmt.Sprint(v...))
}

package mq

import (
	"github.com/HeRedBo/pkg/logx"
)

// LogConfig 日志配置（类型别名，实际由 logx 定义）
// Mode 为空时默认 "console"，Level 为空时默认 "info"
type LogConfig = logx.LogConfig

// InitLogger 根据配置初始化 Logger，委托到 logx.InitLogger
//   - Mode=console: zap development 格式输出到控制台
//   - Mode=file + Rotation=false: zap production JSON 格式输出到指定文件
//   - Mode=file + Rotation=true: 使用 lumberjack 做日志轮转 + zap production JSON encoder
func InitLogger(cfg *LogConfig) (Logger, error) {
	if cfg == nil {
		return logx.GetLogger(), nil
	}
	return logx.InitLogger(*cfg), nil
}

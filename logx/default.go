package logx

import "go.uber.org/zap"

// ─────────────────────────────────────────────
// defaultLogger 零配置默认实现
// 内部使用 zap.NewDevelopmentConfig()，输出到 stdout
// 仅在未注入任何 Logger 时使用，保证零配置可运行
// ─────────────────────────────────────────────

type defaultLogger struct {
	l      *zap.Logger
	fields []zap.Field
}

func newDefaultLogger() *defaultLogger {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{"stdout"}
	l, _ := cfg.Build()
	return &defaultLogger{l: l}
}

func (d *defaultLogger) Info(msg string, fields ...LogField) {
	baseFields := make([]zap.Field, len(d.fields))
	copy(baseFields, d.fields)
	baseFields = append(baseFields, toZapFields(fields)...)
	d.l.Info(msg, baseFields...)
}
func (d *defaultLogger) Warn(msg string, fields ...LogField) {
	baseFields := make([]zap.Field, len(d.fields))
	copy(baseFields, d.fields)
	baseFields = append(baseFields, toZapFields(fields)...)
	d.l.Warn(msg, baseFields...)
}
func (d *defaultLogger) Error(msg string, fields ...LogField) {
	baseFields := make([]zap.Field, len(d.fields))
	copy(baseFields, d.fields)
	baseFields = append(baseFields, toZapFields(fields)...)
	d.l.Error(msg, baseFields...)
}
func (d *defaultLogger) Debug(msg string, fields ...LogField) {
	baseFields := make([]zap.Field, len(d.fields))
	copy(baseFields, d.fields)
	baseFields = append(baseFields, toZapFields(fields)...)
	d.l.Debug(msg, baseFields...)
}
func (d *defaultLogger) WithFields(fields ...LogField) Logger {
	merged := make([]zap.Field, len(d.fields)+len(fields))
	copy(merged, d.fields)
	copy(merged[len(d.fields):], toZapFields(fields))
	return &defaultLogger{l: d.l, fields: merged}
}

// stdDefault 内置默认实例，作为三级优先级的最终兜底
var stdDefault = newDefaultLogger()

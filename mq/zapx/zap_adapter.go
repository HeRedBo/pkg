package zapx

import (
	"go.uber.org/zap"

	"github.com/HeRedBo/pkg/mq"
)

// ZapLogger 将 *zap.Logger 适配为 mq.Logger
type ZapLogger struct {
	l      *zap.Logger
	fields []zap.Field
}

// NewZapLogger 创建 mq.Logger 适配器，将日志委托给底层 *zap.Logger
func NewZapLogger(l *zap.Logger) mq.Logger {
	return &ZapLogger{l: l}
}

func (z *ZapLogger) Info(msg string, fields ...mq.LogField) {
	z.l.Info(msg, append(z.fields, toZapFields(fields)...)...)
}
func (z *ZapLogger) Warn(msg string, fields ...mq.LogField) {
	z.l.Warn(msg, append(z.fields, toZapFields(fields)...)...)
}
func (z *ZapLogger) Error(msg string, fields ...mq.LogField) {
	z.l.Error(msg, append(z.fields, toZapFields(fields)...)...)
}
func (z *ZapLogger) Debug(msg string, fields ...mq.LogField) {
	z.l.Debug(msg, append(z.fields, toZapFields(fields)...)...)
}
func (z *ZapLogger) WithFields(fields ...mq.LogField) mq.Logger {
	merged := make([]zap.Field, len(z.fields)+len(fields))
	copy(merged, z.fields)
	copy(merged[len(z.fields):], toZapFields(fields))
	return &ZapLogger{l: z.l, fields: merged}
}

// toZapFields 将 mq.LogField 转换为 zap.Field
func toZapFields(fields []mq.LogField) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	zf := make([]zap.Field, len(fields))
	for i, f := range fields {
		zf[i] = zap.Any(f.Key, f.Value)
	}
	return zf
}

package zapx

import (
	"go.uber.org/zap"

	"github.com/HeRedBo/pkg/logx"
)

// ZapLogger 将 *zap.Logger 适配为 logx.Logger
type ZapLogger struct {
	l      *zap.Logger
	fields []zap.Field
}

// New 创建 logx.Logger 适配器，将日志委托给底层 *zap.Logger
func New(l *zap.Logger) logx.Logger {
	return &ZapLogger{l: l}
}

func (z *ZapLogger) Info(msg string, fields ...logx.LogField) {
	baseFields := make([]zap.Field, len(z.fields))
	copy(baseFields, z.fields)
	baseFields = append(baseFields, toZapFields(fields)...)
	z.l.Info(msg, baseFields...)
}
func (z *ZapLogger) Warn(msg string, fields ...logx.LogField) {
	baseFields := make([]zap.Field, len(z.fields))
	copy(baseFields, z.fields)
	baseFields = append(baseFields, toZapFields(fields)...)
	z.l.Warn(msg, baseFields...)
}
func (z *ZapLogger) Error(msg string, fields ...logx.LogField) {
	baseFields := make([]zap.Field, len(z.fields))
	copy(baseFields, z.fields)
	baseFields = append(baseFields, toZapFields(fields)...)
	z.l.Error(msg, baseFields...)
}
func (z *ZapLogger) Debug(msg string, fields ...logx.LogField) {
	baseFields := make([]zap.Field, len(z.fields))
	copy(baseFields, z.fields)
	baseFields = append(baseFields, toZapFields(fields)...)
	z.l.Debug(msg, baseFields...)
}
func (z *ZapLogger) WithFields(fields ...logx.LogField) logx.Logger {
	merged := make([]zap.Field, len(z.fields)+len(fields))
	copy(merged, z.fields)
	copy(merged[len(z.fields):], toZapFields(fields))
	return &ZapLogger{l: z.l, fields: merged}
}

// toZapFields 将 logx.LogField 转换为 zap.Field
func toZapFields(fields []logx.LogField) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	zf := make([]zap.Field, len(fields))
	for i, f := range fields {
		zf[i] = zap.Any(f.Key, f.Value)
	}
	return zf
}

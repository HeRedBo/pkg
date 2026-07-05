package logrusx

import (
	"github.com/sirupsen/logrus"

	"github.com/HeRedBo/pkg/mq"
)

// LogrusLogger 将 *logrus.Logger 适配为 mq.Logger
// 内部将 mq.LogField 转换为 logrus.Fields
type LogrusLogger struct {
	l        *logrus.Logger
	preFields logrus.Fields
}

// NewLogrusLogger 创建 logrus 适配器，返回 mq.Logger
func NewLogrusLogger(l *logrus.Logger) mq.Logger {
	return &LogrusLogger{l: l}
}

func (r *LogrusLogger) Info(msg string, fields ...mq.LogField) {
	r.l.WithFields(r.mergeFields(fields)).Info(msg)
}

func (r *LogrusLogger) Warn(msg string, fields ...mq.LogField) {
	r.l.WithFields(r.mergeFields(fields)).Warn(msg)
}

func (r *LogrusLogger) Error(msg string, fields ...mq.LogField) {
	r.l.WithFields(r.mergeFields(fields)).Error(msg)
}

func (r *LogrusLogger) Debug(msg string, fields ...mq.LogField) {
	r.l.WithFields(r.mergeFields(fields)).Debug(msg)
}

func (r *LogrusLogger) WithFields(fields ...mq.LogField) mq.Logger {
	merged := make(logrus.Fields, len(r.preFields))
	for k, v := range r.preFields {
		merged[k] = v
	}
	for _, f := range fields {
		merged[f.Key] = f.Value
	}
	return &LogrusLogger{l: r.l, preFields: merged}
}

// mergeFields 将预绑定字段与调用时字段合并为 logrus.Fields
func (r *LogrusLogger) mergeFields(fields []mq.LogField) logrus.Fields {
	lf := make(logrus.Fields, len(r.preFields)+len(fields))
	for k, v := range r.preFields {
		lf[k] = v
	}
	for _, f := range fields {
		lf[f.Key] = f.Value
	}
	return lf
}

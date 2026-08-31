package db

import (
	"context"
	"fmt"
	"time"

	gormlogger "gorm.io/gorm/logger"

	"github.com/HeRedBo/pkg/logx"
)

// gormLogger 实现 gorm.logger.Interface，将 GORM 日志委托给 logx.Logger
type gormLogger struct {
	logger                    logx.Logger
	sqlLogger                 logx.Logger
	level                     gormlogger.LogLevel
	slowThreshold             time.Duration
	ignoreRecordNotFoundError bool
	enableSqlLog              bool // 是否启用 SQL 日志打印，默认关�?
}

// NewGormLogger 创建 gormLogger 适配器
func NewGormLogger(logger, sqlLogger logx.Logger, slowThreshold time.Duration, enableSqlLog bool) *gormLogger {
	return &gormLogger{
		logger:                    logger,
		sqlLogger:                 sqlLogger,
		level:                     gormlogger.Info,
		slowThreshold:             slowThreshold,
		ignoreRecordNotFoundError: true,
		enableSqlLog:              enableSqlLog,
	}
}

// LogMode 设置日志级别，返回新实例（不可变模式）
func (l *gormLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	newLogger := *l
	newLogger.level = level
	return &newLogger
}

// Info 记录 Info 级别日志
func (l *gormLogger) Info(ctx context.Context, msg string, args ...interface{}) {
	if l.level >= gormlogger.Info {
		l.logger.Info(msg, toLogFields(args)...)
	}
}

// Warn 记录 Warn 级别日志
func (l *gormLogger) Warn(ctx context.Context, msg string, args ...interface{}) {
	if l.level >= gormlogger.Warn {
		l.logger.Warn(msg, toLogFields(args)...)
	}
}

// Error 记录 Error 级别日志
func (l *gormLogger) Error(ctx context.Context, msg string, args ...interface{}) {
	if l.level >= gormlogger.Error {
		l.logger.Error(msg, toLogFields(args)...)
	}
}

// Trace 记录 SQL 执行轨迹
func (l *gormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if l.level <= gormlogger.Silent {
		return
	}

	elapsed := time.Since(begin)

	// 获取 SQL 和受影响行数
	sql, rows := fc()

	// 错误日志
	if err != nil && l.level >= gormlogger.Error {
		fields := []logx.LogField{
			logx.Field("sql", sql),
			logx.Field("rows", rows),
			logx.Field("elapsed", elapsed.String()),
			logx.ErrField(err),
		}
		l.logger.Error("sql trace error", fields...)
		return
	}

	// 慢查询警告
	if l.slowThreshold > 0 && elapsed > l.slowThreshold && l.level >= gormlogger.Warn {
		fields := []logx.LogField{
			logx.Field("sql", sql),
			logx.Field("rows", rows),
			logx.Field("elapsed", elapsed.String()),
		}
		l.sqlLogger.Warn(fmt.Sprintf("slow sql [elapsed > %s]", l.slowThreshold), fields...)
		return
	}

	// 普通SQL日志（需要显式开启）
	if l.enableSqlLog && l.level >= gormlogger.Info {
		fields := []logx.LogField{
			logx.Field("sql", sql),
			logx.Field("rows", rows),
			logx.Field("elapsed", elapsed.String()),
		}
		l.sqlLogger.Info("sql trace", fields...)
	}
}

// toLogFields 将 args 转换为 logx.LogField 切片
func toLogFields(args []interface{}) []logx.LogField {
	if len(args) == 0 {
		return nil
	}
	fields := make([]logx.LogField, 0, len(args)/2)
	for i := 0; i < len(args)-1; i += 2 {
		key, ok := args[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", args[i])
		}
		fields = append(fields, logx.Field(key, args[i+1]))
	}
	// 奇数个参数时，最后一个参数作为额外信息
	if len(args)%2 != 0 {
		fields = append(fields, logx.Field("extra", args[len(args)-1]))
	}
	return fields
}

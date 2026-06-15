package mq

import (
	"fmt"

	"github.com/IBM/sarama"
)

// ─────────────────────────────────────────────
// saramaZapLogger：将 sarama.StdLogger 桥接到 mq.Logger
//
// sarama 内部（分区重平衡、连接建立、offset 提交等）会调用 sarama.Logger
// 通过此适配器，sarama 的底层日志也能统一走 mq.Logger → Zap → 文件/ELK
//
// 使用方式（在业务项目初始化时调用一次）：
//
//	mq.SetSaramaLogger(yourZapLogger)  // 注入后 sarama 内部日志走 Zap
//	mq.SetSaramaLogger(nil)            // 恢复 sarama 默认控制台输出
// ─────────────────────────────────────────────

// saramaZapLogger 实现 sarama.StdLogger，内部代理到 mq.Logger
type saramaZapLogger struct {
	l Logger
}

// Print 实现 sarama.StdLogger
func (s *saramaZapLogger) Print(v ...interface{}) {
	s.l.Debug(fmt.Sprint(v...))
}

// Printf 实现 sarama.StdLogger
func (s *saramaZapLogger) Printf(format string, v ...interface{}) {
	s.l.Debug(fmt.Sprintf(format, v...))
}

// Println 实现 sarama.StdLogger
func (s *saramaZapLogger) Println(v ...interface{}) {
	s.l.Debug(fmt.Sprint(v...))
}

// SetSaramaLogger 将 sarama 内部日志桥接到指定 Logger
// l 传 nil 时恢复 sarama 自带的标准输出
// 建议在 InitSyncKafkaProducer / StartKafkaConsumer 之前调用
func SetSaramaLogger(l Logger) {
	if l == nil {
		// 恢复 sarama 默认行为（输出到 Stdout）
		sarama.Logger = nil
		return
	}
	sarama.Logger = &saramaZapLogger{l: l}
}

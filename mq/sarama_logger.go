package mq

import (
	"fmt"
	"io"
	"log"

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
//	mq.SetSaramaLogger(nil)            // 恢复 sarama 默认丢弃输出
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
// l 传 nil 时恢复 sarama 默认的丢弃输出行为（与 sarama 初始值一致）
// 建议在 InitSyncKafkaProducer / StartKafkaConsumer 之前调用
func SetSaramaLogger(l Logger) {
	if l == nil {
		// 恢复 sarama 默认行为：丢弃所有内部日志（与 sarama 包初始值一致）
		// 注意：不能设为 nil，否则 sarama 内部 debugLogger 代理调用 nil.Println() 会 panic
		sarama.Logger = log.New(io.Discard, "[Sarama] ", log.LstdFlags)
		return
	}
	sarama.Logger = &saramaZapLogger{l: l}
}

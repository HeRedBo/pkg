# mq — Kafka 消息队列封装库使用文档

> **当前版本：v1.0.1** — 修复 `Consumer.Close()` 死锁问题，可安全调用。

## 1. 快速开始

**模块路径：**

```
github.com/HeRedBo/pkg/mq
```

**安装：**

```bash
go get github.com/HeRedBo/pkg/mq
```

**基本依赖：**

- Kafka 集群环境
- [sarama](https://github.com/IBM/sarama) — Kafka Go 客户端库
- [go-resiliency](https://github.com/eapache/go-resiliency) — 熔断器
- [zap](https://go.uber.org/zap) — 结构化日志库

---

## 2. 同步生产者

### 2.1 初始化

```go
// 使用默认配置（传 nil）
err := mq.InitSyncKafkaProducer("my-sync-producer", []string{"localhost:9092"}, nil)
if err != nil {
    log.Fatal(err)
}

// 使用自定义配置
config := sarama.NewConfig()
config.Version = sarama.V4_0_0_0
config.Producer.Return.Successes = true
config.Producer.RequiredAcks = sarama.WaitForAll
err := mq.InitSyncKafkaProducer("my-sync-producer", []string{"localhost:9092"}, config)
```

### 2.2 发送单条消息

```go
producer := mq.GetKafkaSyncProducer("my-sync-producer")

msg := &sarama.ProducerMessage{
    Topic: "my-topic",
    Key:   sarama.StringEncoder("my-key"),
    Value: mq.KafkaMsgValueStrEncoder("hello world"),
    // 也可以使用字节数组：
    // Value: mq.KafkaMsgValueEncoder([]byte("hello world")),
}

partition, offset, err := producer.Send(msg)
if err != nil {
    log.Printf("send failed: %v", err)
} else {
    log.Printf("sent to partition=%d, offset=%d", partition, offset)
}
```

> **注意：** `Send` 返回值为 `(partition int32, offset int64, err error)`。

### 2.3 批量发送消息

```go
producer := mq.GetKafkaSyncProducer("my-sync-producer")

msgs := []*sarama.ProducerMessage{
    {Topic: "my-topic", Key: sarama.StringEncoder("k1"), Value: mq.KafkaMsgValueStrEncoder("msg1")},
    {Topic: "my-topic", Key: sarama.StringEncoder("k2"), Value: mq.KafkaMsgValueStrEncoder("msg2")},
}

errs := producer.SendMessages(msgs)
if errs != nil {
    for _, e := range errs {
        log.Printf("send error: %v", e.Err)
    }
}
```

### 2.4 关闭

```go
producer := mq.GetKafkaSyncProducer("my-sync-producer")
producer.Close()
```

> `Close` 是幂等的，多次调用不会报错。

### 2.5 注入自定义日志

```go
zapLogger, _ := zap.NewProduction()

// 通过 Option 注入（仅当前生产者生效）
err := mq.InitSyncKafkaProducer("my-sync-producer", []string{"localhost:9092"}, nil, mq.WithLogger(zapLogger))
```

---

## 3. 异步生产者

### 3.1 初始化

```go
err := mq.InitAsyncKafkaProducer("my-async-producer", []string{"localhost:9092"}, nil)
if err != nil {
    log.Fatal(err)
}
```

### 3.2 发送消息

```go
producer := mq.GetKafkaAsyncProducer("my-async-producer")

msg := &sarama.ProducerMessage{
    Topic: "my-topic",
    Key:   sarama.StringEncoder("my-key"),
    Value: mq.KafkaMsgValueStrEncoder("async hello"),
}

// 异步发送：消息写入 Input 通道，由后台 goroutine 批量发送
err := producer.Send(msg)
if err != nil {
    log.Printf("enqueue failed: %v", err)
}
```

> **异步特性：** `Send` 仅将消息写入 `Input` 通道即返回，不等待 Kafka 确认。发送成功/失败由内部 `check` goroutine 自动收集处理。

### 3.3 关闭

```go
producer := mq.GetKafkaAsyncProducer("my-async-producer")
producer.Close()
```

---

## 4. 消费者

### 4.1 定义消息处理回调

```go
// KafkaMessageHandler 签名：func(message *sarama.ConsumerMessage) (bool, error)
handler := func(message *sarama.ConsumerMessage) (bool, error) {
    // 处理业务逻辑
    fmt.Printf("received: topic=%s partition=%d offset=%d key=%s value=%s\n",
        message.Topic, message.Partition, message.Offset,
        string(message.Key), string(message.Value))

    // 返回 (true, nil)  → 处理成功，自动标记 offset
    // 返回 (false, err) → 处理失败，不标记 offset
    return true, nil
}
```

### 4.2 启动消费者

```go
// 使用默认配置
consumer, err := mq.StartKafkaConsumer(
    []string{"localhost:9092"}, // brokers
    []string{"my-topic"},       // topics
    "my-consumer-group",        // groupID
    nil,                        // config（nil 使用默认配置）
    handler,                    // 消息处理回调
)
if err != nil {
    log.Fatal(err)
}

// 使用自定义配置
config := sarama.NewConfig()
config.Version = sarama.V4_0_0_0
config.Consumer.Offsets.Initial = sarama.OffsetOldest
config.Consumer.Return.Errors = true

consumer, err := mq.StartKafkaConsumer(
    []string{"localhost:9092"},
    []string{"my-topic"},
    "my-consumer-group",
    config,
    handler,
)
```

### 4.3 关闭

```go
consumer.Close()
```

> `Close` 是幂等的，多次调用不会报错。内部会取消消费上下文并等待消费 goroutine 退出。

### 4.4 自动重连机制

消费者内置了**熔断器 + 后台重连**机制，Kafka 集群短暂不可用时无需人工干预：

**触发条件：** 消费过程中遇到 `sarama.ErrOutOfBrokers` 或 `sarama.ErrNotConnected` 时，自动标记为断开并触发重连。

**重连流程：**

1. 后台 `keepConnect` goroutine 监听到断开信号
2. 通过熔断器（3 次失败后熔断，熔断后 3 秒半开）尝试重新连接
3. 连接成功后自动重启消费 goroutine
4. 若熔断器打开，等待 5 秒后再次重试

**注意事项：**

- 重连期间消息不会丢失，恢复连接后从上次已标记的 offset 继续消费
- 收到 `SIGINT`/`SIGTERM` 信号后，重连循环自动退出，不会无限重试
- 重连过程的日志可通过 `WithLogger` 或 `SetLogger` 输出到文件，便于监控

### 4.5 注入自定义日志

```go
zapLogger, _ := zap.NewProduction()

consumer, err := mq.StartKafkaConsumer(
    []string{"localhost:9092"},
    []string{"my-topic"},
    "my-consumer-group",
    nil,
    handler,
    mq.WithLogger(zapLogger), // Option 注入
)
```

---

## 5. 自定义日志配置

### 5.1 Logger 接口

```go
type Logger interface {
    Info(msg string, fields ...zap.Field)
    Warn(msg string, fields ...zap.Field)
    Error(msg string, fields ...zap.Field)
    Debug(msg string, fields ...zap.Field)
}
```

`*zap.Logger` 天然满足此接口，无需额外适配器。

### 5.2 方式一：全局注入

所有组件（生产者、消费者）共用同一个 Logger：

```go
zapLogger, _ := zap.NewProduction()
mq.SetLogger(zapLogger)
```

### 5.3 方式二：Option 注入

单个组件使用独立的 Logger：

```go
zapLogger, _ := zap.NewProduction()
mq.InitSyncKafkaProducer("p1", hosts, nil, mq.WithLogger(zapLogger))
```

### 5.4 优先级

```
Option 注入 > 全局 SetLogger > 默认控制台
```

- 未做任何注入时，日志输出到控制台（使用标准库 `log`）
- 调用 `SetLogger` 后，所有组件使用全局 Logger
- 通过 `WithLogger` Option 注入的 Logger 优先级最高，仅对当前组件生效

### 5.5 文件日志示例

参考 `mq_logger_test.go` 中的思路，生产环境推荐使用文件日志：

```go
func newFileLogger(logPath string) (*zap.Logger, error) {
    // 日志切割配置（需配合 lumberjack）
    writer := &lumberjack.Logger{
        Filename:   logPath,
        MaxSize:    100,  // MB
        MaxBackups: 30,
        MaxAge:     7,    // 天
        Compress:   true,
    }

    core := zapcore.NewCore(
        zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
        zapcore.AddSync(writer),
        zapcore.InfoLevel,
    )
    return zap.New(core), nil
}

// 使用
logger, _ := newFileLogger("/var/log/myapp/kafka.log")
mq.SetLogger(logger)
```

### 5.6 Sarama 内部日志桥接

sarama 库内部（分区重平衡、连接建立、offset 提交等）也会产生日志，通过 `SetSaramaLogger` 可将其桥接到你的 Logger：

```go
zapLogger, _ := zap.NewProduction()
mq.SetSaramaLogger(zapLogger)

// 传 nil 恢复 sarama 默认控制台输出
mq.SetSaramaLogger(nil)
```

---

## 6. 配置说明

### 6.1 生产者默认配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `Version` | `V4_0_0_0` | Kafka 版本 |
| `Net.DialTimeout` | 30s | 初始连接超时 |
| `Net.WriteTimeout` | 30s | 写入超时 |
| `Net.ReadTimeout` | 30s | 读取超时 |
| `Producer.Retry.Max` | 5 | 最大重试次数 |
| `Producer.Retry.Backoff` | 500ms | 重试间隔 |
| `Producer.Return.Successes` | true | 返回成功消息（同步必须） |
| `Producer.Return.Errors` | true | 返回错误消息 |
| `Producer.MaxMessageBytes` | 2,000,000 | 单条消息最大字节数 |
| `Producer.RequiredAcks` | `WaitForAll` | 等待所有副本确认 |
| `Producer.Partitioner` | `HashPartitioner` | 分区策略 |
| `Producer.Compression` | `CompressionLZ4` | 压缩算法（LZ4 性价比最高） |
| `Producer.Flush.Frequency` | 500ms | 批量发送频率 |
| `Producer.Flush.MaxMessages` | 1000 | 批量最大消息数 |

### 6.2 消费者默认配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `Version` | `V4_0_0_0` | Kafka 版本 |
| `Consumer.Return.Errors` | true | 返回错误消息 |
| `Consumer.Offsets.Initial` | `OffsetNewest` | 初始 offset 策略（最新消息） |
| `Consumer.Group.Session.Timeout` | 20s | 会话超时 |
| `Consumer.Group.Heartbeat.Interval` | 6s | 心跳间隔 |
| `Consumer.MaxProcessingTime` | 500ms | 单次处理最大时间 |
| `Consumer.Fetch.Default` | 2MB | 默认拉取字节数 |

### 6.3 常用自定义配置项

```go
config := sarama.NewConfig()
config.Version = sarama.V4_0_0_0

// 生产者相关
config.Producer.RequiredAcks = sarama.WaitForLocal       // 仅等待首领副本确认（更快）
config.Producer.Partitioner = sarama.NewRoundRobinPartitioner // 轮询分区
config.Producer.Compression = sarama.CompressionZstd      // 更高压缩比

// 消费者相关
config.Consumer.Offsets.Initial = sarama.OffsetOldest     // 从最早消息开始消费
config.Consumer.Group.Rebalance.GroupStrategies = ...     // 自定义重平衡策略
```

---

## 7. 最佳实践

### 7.1 初始化顺序

建议按以下顺序初始化：

```go
// 1. 先配置日志
zapLogger, _ := zap.NewProduction()
mq.SetLogger(zapLogger)
mq.SetSaramaLogger(zapLogger)

// 2. 再初始化生产者/消费者
mq.InitSyncKafkaProducer("my-producer", hosts, nil)
consumer, _ := mq.StartKafkaConsumer(hosts, topics, groupID, nil, handler)

// 3. 业务逻辑...

// 4. 优雅退出
consumer.Close()
mq.GetKafkaSyncProducer("my-producer").Close()
```

### 7.2 连接管理

- **Close 幂等：** `SyncProducer.Close()`、`AsyncProducer.Close()`、`Consumer.Close()` 均可安全多次调用
- **自动重连：** 生产者/消费者内置熔断器 + 重连机制，Kafka 短暂不可用时会自动恢复
- **优雅退出：** 内部监听 `SIGINT`/`SIGTERM` 等信号，收到信号后自动停止重连 goroutine

### 7.3 错误处理

- **同步生产者：** `Send` 返回 `error`，`SendMessages` 返回 `sarama.ProducerErrors`，需逐条检查
- **异步生产者：** `Send` 仅返回入队错误，实际发送错误由内部 `check` goroutine 自动记录日志；遇到 `ErrOutOfBrokers`/`ErrNotConnected` 会自动触发重连
- **消费者：** 消费过程中遇到 `ErrOutOfBrokers`/`ErrNotConnected` 会自动触发重连（详见第 4.4 节）

### 7.4 日志配置建议

- **开发环境：** 使用默认控制台日志即可，零配置
- **生产环境：** 强烈建议使用文件日志（配合 `lumberjack` 做日志切割），便于排查问题和接入 ELK

---

## 8. API 速查表

| 函数/方法 | 签名 | 说明 |
|-----------|------|------|
| `InitSyncKafkaProducer` | `func InitSyncKafkaProducer(name string, hosts []string, config *sarama.Config, opts ...Option) error` | 初始化同步生产者 |
| `InitAsyncKafkaProducer` | `func InitAsyncKafkaProducer(name string, hosts []string, config *sarama.Config, opts ...Option) error` | 初始化异步生产者 |
| `GetKafkaSyncProducer` | `func GetKafkaSyncProducer(name string) *SyncProducer` | 获取同步生产者实例 |
| `GetKafkaAsyncProducer` | `func GetKafkaAsyncProducer(name string) *AsyncProducer` | 获取异步生产者实例 |
| `SyncProducer.Send` | `func (sp *SyncProducer) Send(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)` | 同步发送单条消息 |
| `SyncProducer.SendMessages` | `func (sp *SyncProducer) SendMessages(msgs []*sarama.ProducerMessage) sarama.ProducerErrors` | 同步批量发送消息 |
| `SyncProducer.Close` | `func (sp *SyncProducer) Close() error` | 关闭同步生产者（幂等） |
| `AsyncProducer.Send` | `func (ap *AsyncProducer) Send(msg *sarama.ProducerMessage) error` | 异步发送消息（写入 Input 通道） |
| `AsyncProducer.Close` | `func (ap *AsyncProducer) Close() error` | 关闭异步生产者（幂等） |
| `StartKafkaConsumer` | `func StartKafkaConsumer(hosts, topics []string, groupID string, config *sarama.Config, f KafkaMessageHandler, opts ...Option) (*Consumer, error)` | 启动消费者 |
| `Consumer.Close` | `func (c *Consumer) Close() error` | 关闭消费者（幂等，等待 goroutine 退出） |
| `SetLogger` | `func SetLogger(l Logger)` | 全局注入 Logger |
| `SetSaramaLogger` | `func SetSaramaLogger(l Logger)` | 桥接 sarama 内部日志到 Logger |
| `WithLogger` | `func WithLogger(l Logger) Option` | Option 注入 Logger（优先级最高） |
| `KafkaMsgValueEncoder` | `func KafkaMsgValueEncoder(value []byte) sarama.Encoder` | 字节数组消息编码器 |
| `KafkaMsgValueStrEncoder` | `func KafkaMsgValueStrEncoder(value string) sarama.Encoder` | 字符串消息编码器 |

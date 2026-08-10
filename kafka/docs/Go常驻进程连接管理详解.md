# Go 常驻进程连接管理详解 — 以 Kafka 模块为例

## 一、PHP 与 Go 运行模型的根本差异

### PHP：请求级生命周期

- 每个请求 = 一次短命进程
- 连接（MySQL/Redis/Kafka）在请求开始时创建，请求结束时自动释放
- 不存在"连接失效"问题，因为连接用完就扔
- 不需要重试/重连/熔断/信号检测

```
用户请求 → PHP-FPM 分配进程 → 初始化连接 → 处理业务 → 释放连接 → 进程结束
```

### Go：常驻进程生命周期

- 进程启动后长期驻留内存
- 连接在启动时创建，被成千上万个请求共享复用
- 连接可能因为网络抖动、服务端重启、防火墙超时等原因"悄悄死掉"
- 必须自己管理连接的生命周期

```
Go 进程启动 → 初始化连接池 → 处理请求1 → 请求2 → ... → 请求N → 进程退出
         ↑_________同一个进程，同一批连接，持续运行_________↑
```

## 二、四大核心机制及其必要性

### 2.1 重试（Retry）

**为什么需要：** 常驻进程中，单次操作失败不代表永远失败。临时性故障（broker 重启、网络抖动、主从切换）很常见，重试一下就好了。

**PHP 对比：** PHP 中连接失败直接报错，下次请求再试，用户无感知。

**在 Kafka 中的体现：**
- Sarama 内置请求级重试：`Producer.Retry.Max = 5`，`Producer.Retry.Backoff = 500ms`
- 单条消息发送失败 → 换 broker 重试 → 最多 5 次

### 2.2 重连（Reconnect）

**为什么需要：** 重试解决"单次请求失败"，但如果整个连接都断了（如 MySQL 服务重启），重试多少次都没用。需要销毁旧连接、建立新连接。

**PHP 对比：** 每次请求都是新连接，天然不存在这个问题。

**在 Kafka 中的体现：**
- mq 模块检测 `ErrOutOfBrokers` / `ErrNotConnected` 等连接级错误
- 状态机切换：connected → disconnected
- 自动销毁旧连接，尝试重建新连接
- 重连成功后恢复消费/生产

### 2.3 熔断器（Circuit Breaker）

**为什么需要：** 如果 Kafka 集群真的宕机了（不是临时故障），没有熔断器的话程序会疯狂重连，导致日志刷屏、CPU 飙升、触发连接限流。熔断器在连续失败后暂停重试，保护系统资源。

**PHP 对比：** 单次请求，不存在持续疯狂重试的场景。

**熔断器三种状态：**
```
Closed（正常）→ 连续失败3次 → Open（熔断，拒绝所有请求）
                                    ↓
                              等待2-5秒
                                    ↓
                              Half-Open（放一个请求试探）
                                    ↓
                        成功 → Closed    失败 → Open
```

**在 Kafka 中的体现：**
- 使用 `github.com/eapache/go-resiliency/breaker` 包
- 保护重连逻辑，防止无效重连消耗资源

### 2.4 信号检测（Signal Detection）

**为什么需要：** Go 常驻进程不会自动退出。直接 kill -9 会中断正在处理的请求（Kafka offset 没提交、MySQL 事务没完成）。需要捕获 OS 信号，实现优雅退出。

**PHP 对比：** 请求结束 = 进程清理，不需要这个流程。

**在 Kafka 中的体现：**
- `watchSignals` 监听 SIGTERM/SIGINT
- 收到信号后：停止接收新请求 → 等待处理中消息完成 → 提交 offset → 关闭连接 → 进程退出

## 三、熔断器详解 — mq 模块实现分析

### 3.1 熔断器初始化参数

`breaker.New(errorThreshold, successThreshold, timeout)` 接受三个参数：

| 参数 | 含义 |
|------|------|
| `errorThreshold` | 连续失败次数达到此值后，熔断器从 Closed → Open |
| `successThreshold` | 半开状态下连续成功次数达到此值后，熔断器从 Half-Open → Closed |
| `timeout` | 熔断器打开后等待多久进入 Half-Open 状态 |

### 3.2 生产者与消费者参数对比

| 参数 | 同步生产者 | 异步生产者 | 消费者 | 含义 |
|------|-----------|-----------|--------|------|
| errorThreshold | 3 | 3 | 3 | 连续失败 3 次后熔断 |
| successThreshold | 1 | 1 | 1 | 半开状态 1 次成功即恢复 |
| timeout | 2s | 2s | 3s | 熔断后等待多久进入半开 |

> 消费者的 timeout 为 3 秒（比生产者长 1 秒），因为消费者重连涉及 ConsumerGroup 的分区重平衡，需要更多时间让 Broker 端释放资源。

### 3.3 熔断器在重连中的调用位置

**生产者**：`baseKeepConnect` 方法中通过 `kp.Breaker.Run(connect)` 调用
**消费者**：`keepConnect` 方法中通过 `c.breaker.Run(func() error { return c.connect() })` 调用

### 3.4 完整状态流转

```
┌──────────────────────────────────────────┐
│         Closed（正常状态）                │
│  breaker.Run(connect) 正常执行连接函数     │
└──────────┬───────────────────────────────┘
           │ 连续失败 3 次（errorThreshold=3）
           ▼
┌──────────────────────────────────────────┐
│         Open（熔断状态）                   │
│  breaker.Run() 立即返回 ErrBreakerOpen     │
│  连接函数不会被执行                         │
│                                          │
│  处理逻辑：                                │
│  - 日志告警                                │
│  - 检查是否仍为 disconnected 且未 closed    │
│  - 延迟等待（生产者2s / 消费者5s）          │
│  - 重新发送 ReConnect 信号                  │
└──────────┬───────────────────────────────┘
           │ 等待 timeout（生产者2s / 消费者3s）后自动进入
           ▼
┌──────────────────────────────────────────┐
│       Half-Open（半开状态）                │
│  breaker.Run(connect) 再次执行连接函数      │
│                                          │
│  ┌─ 成功 → Closed（恢复正常，退出重连循环） │
│  │                                        │
│  └─ 失败 → Open（重新熔断，再次延迟重试）   │
└──────────────────────────────────────────┘
```

### 3.5 两层保护机制

- **内层**：熔断器自身的 `timeout`（生产者 2s / 消费者 3s）控制 Open → Half-Open 的转换
- **外层**：`time.AfterFunc`（生产者 2s / 消费者 5s）在熔断器打开后延迟重新发送重连信号

### 3.6 断连触发链路

```
Kafka 集群不可用 / 网络中断
    ↓
[生产者] Send/SendMessages 返回 ErrBrokerNotAvailable
    或 ErrOutOfBrokers / ErrNotConnected
[消费者] Consume 返回 ErrOutOfBrokers / ErrNotConnected
    ↓
状态变更: connected → disconnected（加锁保护）
    ↓
发送信号到 ReConnect channel
    ↓
keepConnect / baseKeepConnect 收到信号
    ↓
进入重连循环，每次调用 breaker.Run(connect)
```

## 四、Sarama 内置重试 vs mq 模块重连 — 互补关系

```
┌─────────────────────────────────────────────────────────┐
│                    mq 模块（连接级）                      │
│                                                         │
│  检测 ErrOutOfBrokers / ErrNotConnected                 │
│       ↓                                                 │
│  状态机切换: connected → disconnected                    │
│       ↓                                                 │
│  熔断器保护重连（3次失败后熔断，防止疯狂重试）              │
│       ↓                                                 │
│  重连成功后: disconnected → connected                    │
│       ↓                                                 │
│  自动恢复消费/生产                                       │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │          Sarama 内置（请求级）                      │  │
│  │                                                   │  │
│  │  单条消息发送失败 → 换 broker 重试                  │  │
│  │  最多重试 5 次，间隔 500ms                         │  │
│  │  全部失败 → 返回错误给 mq 模块                     │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

| 场景 | Sarama 内置重试 | mq 模块额外机制 |
|------|----------------|----------------|
| 单个请求临时失败（网络抖动） | 自动重试 5 次，间隔 500ms | 不需要额外处理 |
| 所有 broker 不可用（ErrOutOfBrokers） | 重试耗尽后返回错误 | 自动检测 → 状态转 disconnected → 熔断器保护重连 |
| 连接完全断开（ErrNotConnected） | 同上 | 同上 |
| Broker 不可用（ErrBrokerNotAvailable） | 同上 | 同步生产者自动触发重连 |
| 消费循环中断 | sarama 内部会重启 session | mq 模块有外层循环 + 断连检测兜底 |
| 进程优雅退出 | 无 | watchSignals + close(exit) 广播 |

## 五、总结对比表

| 机制 | PHP 为什么不需要 | Go 为什么必须 | Kafka 模块中的体现 |
|------|-----------------|--------------|-------------------|
| 重试 | 连接用完就扔，失败就报错 | 临时故障常见，重试即可恢复 | Sarama: Producer.Retry.Max=5 |
| 重连 | 每次请求都是新连接 | 长连接会断，需要重建 | 状态机 + 自动重连逻辑 |
| 熔断器 | 单次请求，不存在持续重试 | 防止连接级故障时无限重试 | eapache/go-resiliency/breaker |
| 信号检测 | 请求结束自动清理 | 常驻进程需要优雅退出 | watchSignals + exit channel |

## 六、一句话总结

**PHP 是"一次性"思维 — 连接用完就扔，所有问题在下次请求中自然解决。Go 是"常驻进程"思维 — 连接要长期维护，必须自己处理连接失效、临时故障、优雅退出这些"生命周期管理"问题。重试、重连、熔断、信号检测，本质上都是常驻进程为了维护连接健康而必须做的事情。**

## 七、相关依赖

| 包名 | 用途 |
|------|------|
| `github.com/IBM/sarama` | Kafka Go 客户端，内置请求级重试 |
| `github.com/eapache/go-resiliency/breaker` | 熔断器实现，保护重连逻辑 |

## 八、项目中各模块的体现

| 模块路径 | 使用的机制 |
|---------|-----------|
| `pkg/mq/` | 重试 + 重连 + 熔断器 + 信号检测（生产级 Kafka 封装） |
| `pkg/kafka/` | Sarama 内置重试 + 简单 sleep 循环重连（学习/原型代码） |
| `pkg/redis/` | 连接池自动重连 |
| `pkg/db/` | MySQL 驱动内置重连 + 连接池 |
| `pkg/httpclient/` | 自定义重试逻辑（retry.go） |

## 九、第三方包的内置能力 — 大部分优秀包已经帮你做了

### 9.1 各主流包内置能力对比

| 包/库 | 内置重试 | 内置重连 | 内置连接池 | 你需要额外做的 |
|-------|---------|---------|-----------|--------------|
| `database/sql`（标准库） | 部分 | 自动 | 有 | 几乎不需要 |
| `go-redis/redis` | 有 | 自动 | 有 | 几乎不需要 |
| `IBM/sarama`（Kafka） | 有（请求级） | **消费者不完整** | 有 | **需要补连接级重连** |
| `go-sql-driver/mysql` | 有 | 配合 database/sql | 有 | 几乎不需要 |
| `net/http`（标准库） | 部分 | N/A | N/A | 按需加重试 |
| `google.golang.org/grpc` | 有 | 有（keepalive） | 有 | 几乎不需要 |
| `go.mongodb.org/mongo-driver` | 有 | 自动 | 有 | 几乎不需要 |
| `streadway/amqp`（RabbitMQ） | **无** | **无** | **无** | **需要自己封装全套** |

**结论：越成熟的包，内置的容错机制越完善。但"成熟度"参差不齐，不能一概而论。**

### 9.2 实际案例对比

#### MySQL：database/sql 帮你做完了所有事

```go
// 你只需要这样写，其他什么都不用管
db, _ := sql.Open("mysql", dsn)
db.SetMaxIdleConns(10)
db.SetMaxOpenConns(100)

// 查询时如果连接断了，database/sql 内部自动：
// 1. 检测到连接失效
// 2. 从连接池中取另一个可用连接
// 3. 如果没有可用连接，自动创建新连接
// 4. 重试查询
rows, _ := db.Query("SELECT * FROM users")
```

**不需要写任何重试/重连代码。**

#### Redis：go-redis 也帮你做完了

```go
rdb := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    PoolSize: 20,        // 连接池
    MaxRetries: 3,       // 内置重试！
    MinRetryBackoff: 8ms,
    MaxRetryBackoff: 512ms,
})

rdb.Get(ctx, "key")  // 断了自动重连，失败了自动重试
```

#### Kafka：sarama 只做了一半

```go
// sarama 生产者 — 内置重试，基本够用
config.Producer.Retry.Max = 5        // ✅ 有重试
config.Producer.Return.Successes = true

// sarama 消费者 — 连接级重连不完整！
// Consume() 返回错误后，如果你不重新调用，消费就停了
// ❌ 没有自动重连
// ❌ 没有熔断器
// ❌ 没有状态管理
```

**这就是为什么 mq 模块需要自己封装重连 + 熔断器 — 因为 sarama 在消费者端只做了"请求级"的重试，没做"连接级"的恢复。**

### 9.3 你的 mq 模块做的 — 正好是 sarama 没覆盖的部分

```
sarama 已经做的（不用管）              mq 模块补上的（sarama 没做的）
┌──────────────────────┐           ┌──────────────────────────┐
│ ✅ 请求级重试（5次）   │           │ ✅ 连接级重连              │
│ ✅ Broker 自动切换    │           │ ✅ 状态机管理              │
│ ✅ 连接池（生产者）    │           │ ✅ 熔断器保护              │
│ ✅ 元数据自动刷新     │           │ ✅ 优雅退出（信号检测）     │
│                      │           │ ✅ 消费循环自动恢复        │
└──────────────────────┘           └──────────────────────────┘
```

## 十、实战决策清单 — 拿到一个新第三方包时怎么用

### 10.1 判断标准：检查包是否内置了三个能力

| 检查项 | 怎么判断 | 去哪里看 |
|--------|---------|----------|
| **连接池管理** | 是否有 `SetMaxIdleConns`、`PoolSize` 等配置？ | README / godoc |
| **自动重连** | 断开后是否能自动恢复？还是需要调用方手动重建连接？ | 源码中搜 `reconnect`、`retry`、`dial` |
| **重试策略** | 是否有 `RetryMax`、`Backoff` 等配置项？ | Config 结构体 |

**三个都有 → 直接用，不用封装**
**缺一个或多个 → 需要在业务层补上**

### 10.2 决策流程图

```
拿到一个第三方包
    │
    ├─ 1. 看 README / godoc，找关键词：
    │     retry / reconnect / pool / circuit-breaker / keepalive
    │
    ├─ 2. 看 Config 结构体，有没有这些字段：
    │     RetryMax / Backoff / PoolSize / MaxRetries / AutoReconnect
    │
    ├─ 3. 看 Issue / 社区讨论：
    │     搜 "reconnect" "connection lost" "retry" 看别人怎么处理的
    │
    └─ 4. 根据结果决策：
          │
          ├─ 全都有 → 直接用，配置好参数就行
          │          例：database/sql、go-redis、grpc
          │
          ├─ 有部分 → 补上缺的
          │          例：sarama（有重试，缺连接级重连 → mq 模块补上了）
          │
          └─ 都没有 → 需要完整封装
                     例：streadway/amqp（RabbitMQ）
                     需要自己加：重试 + 重连 + 熔断 + 信号检测
```

### 10.3 看错误类型判断是否需要额外处理

```go
// 情况A：包返回的是"临时性错误"，内部已经重试过了
// → 不需要再加 retry
err := db.Query("SELECT ...")  
// database/sql 内部已经处理了重连和重试

// 情况B：包返回的是"永久性错误"，告诉你"我搞不定了"
// → 需要判断是连接级故障还是真的业务错误
err := producer.Send(msg)
// sarama 重试 5 次都失败了，返回 ErrOutOfBrokers
// 这时候需要判断：连接废了 → 触发重连
```

## 十一、总结

**优秀的 Go 第三方包（database/sql、go-redis、grpc）确实已经内置了重试/重连/连接池，大部分场景直接用就行。但并非所有包都这么完善 — 关键看它是否覆盖了"连接级故障恢复"。sarama 就是一个典型：请求级重试做得很好，但连接级重连需要自己补。mq 模块做的事情，正好就是填补 sarama 没覆盖的那一块。判断标准很简单：看 Config 里有没有 RetryMax、PoolSize、AutoReconnect 这些字段，没有的部分就是需要封装的部分。**

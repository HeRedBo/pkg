# Kafka 生产者封装 — 架构梳理与优化方向

## 一、整体结构

```mermaid
graph TD
    A["KafkaProducer (基类结构体)"] --> B["SyncProducer (同步生产者)"]
    A --> C["AsyncProducer (异步生产者)"]

    B --> D["sarama.SyncProducer"]
    C --> E["sarama.AsyncProducer"]

    B --> F["keepConnect()"]
    B --> G["check()"]

    C --> H["keepConnect()"]
    C --> I["check()"]

    J["InitSyncKafkaProducer()"] --> B
    K["InitAsyncKafkaProducer()"] --> C
```

### 核心文件

| 文件 | 职责 |
|------|------|
| `kafka_producer.go` | 生产者封装：SyncProducer / AsyncProducer 的初始化、重连、状态检查、消息发送 |
| `logger.go` | 日志抽象：Logger 接口、全局注入、Option 模式注入 |

### 结构体关系

```
KafkaProducer          ← 公共字段（Name, Hosts, Config, Status, Breaker, ReConnect, StatusLock, log）
├── SyncProducer       ← 嵌入 KafkaProducer + *sarama.SyncProducer
└── AsyncProducer      ← 嵌入 KafkaProducer + *sarama.AsyncProducer
```

---

## 二、SyncProducer 流程图

```mermaid
graph TD
    START["InitSyncKafkaProducer()"] --> INIT["创建 SyncProducer 结构体"]
    INIT --> OPT["applyOptions: 解析 Logger 等选项"]
    OPT --> CFG{"config == nil?"}
    CFG -- 是 --> DEFAULT["使用 getDefaultProducerConfig()"]
    CFG -- 否 --> CUSTOM["使用传入 config"]
    DEFAULT --> CONNECT["sarama.NewSyncProducer()"]
    CUSTOM --> CONNECT
    CONNECT --> OK{"连接成功?"}
    OK -- 否 --> ERR["返回 error"]
    OK -- 是 --> REG["注册到 KafkaSyncProducers map"]
    REG --> GOROUTINE1["go keepConnect()"]
    REG --> GOROUTINE2["go check()"]
    REG --> LOG["log: SyncKafkaProducer connected"]
```

### SyncProducer.keepConnect()

```mermaid
graph TD
    KC_START["进入无限循环"] --> KC_CHECK{"Status == Closed?"}
    KC_CHECK -- 是 --> KC_EXIT["return 退出"]
    KC_CHECK -- 否 --> KC_SELECT["select 监听"]
    KC_SELECT --> KC_SIG["signals: 收到系统信号"]
    KC_SELECT --> KC_RE["ReConnect 通道收到信号"]
    KC_SIG --> KC_CLOSE["Status = Closed, return"]
    KC_RE --> KC_DIS{"Status == Disconnected?"}
    KC_DIS -- 否 --> KC_SELECT
    KC_DIS -- 是 --> KC_BREAKER["breaker.Run: 尝试 NewSyncProducer"]
    KC_BREAKER --> KC_RES{"结果?"}
    KC_RES -- 成功 --> KC_OK["Status = Connected, 更新 producer"]
    KC_OK --> KC_SELECT
    KC_RES -- BreakerOpen --> KC_WAIT["2s 后发送 ReConnect 信号"]
    KC_WAIT --> KC_SELECT
    KC_RES -- 其他错误 --> KC_BREAKER
```

### SyncProducer.check()

```mermaid
graph TD
    CK_START["进入无限循环"] --> CK_CHECK{"Status == Closed?"}
    CK_CHECK -- 是 --> CK_EXIT["return 退出"]
    CK_CHECK -- 否 --> CK_SELECT["select 监听 signals"]
    CK_SELECT --> CK_SIG["收到系统信号"]
    CK_SIG --> CK_CLOSE["Status = Closed, return"]
```

> **注意：SyncProducer.check() 的唯一职责就是监听系统信号，而这个功能在 keepConnect 中已经存在。**

---

## 三、AsyncProducer 流程图

```mermaid
graph TD
    START["InitAsyncKafkaProducer()"] --> INIT["创建 AsyncProducer 结构体"]
    INIT --> OPT["applyOptions: 解析 Logger 等选项"]
    OPT --> CFG{"config == nil?"}
    CFG -- 是 --> DEFAULT["使用 getDefaultProducerConfig()"]
    CFG -- 否 --> CUSTOM["使用传入 config"]
    DEFAULT --> CONNECT["sarama.NewAsyncProducer()"]
    CUSTOM --> CONNECT
    CONNECT --> OK{"连接成功?"}
    OK -- 否 --> ERR["返回 error"]
    OK -- 是 --> REG["注册到 KafkaAsyncProducers map"]
    REG --> GOROUTINE1["go keepConnect()"]
    REG --> GOROUTINE2["go check()"]
    REG --> LOG["log: AsyncKafkaProducer connected"]
```

### AsyncProducer.keepConnect()

```mermaid
graph TD
    KC_START["进入无限循环"] --> KC_CHECK{"Status == Closed?"}
    KC_CHECK -- 是 --> KC_EXIT["return 退出"]
    KC_CHECK -- 否 --> KC_SELECT["select 监听"]
    KC_SELECT --> KC_SIG["signals: 收到系统信号"]
    KC_SELECT --> KC_RE["ReConnect 通道收到信号"]
    KC_SIG --> KC_CLOSE["Status = Closed, return"]
    KC_RE --> KC_DIS{"Status == Disconnected?"}
    KC_DIS -- 否 --> KC_SELECT
    KC_DIS -- 是 --> KC_BREAKER["breaker.Run: 尝试 NewAsyncProducer"]
    KC_BREAKER --> KC_RES{"结果?"}
    KC_RES -- 成功 --> KC_OK["Status = Connected, 更新 producer"]
    KC_OK --> KC_SELECT
    KC_RES -- BreakerOpen --> KC_WAIT["2s 后发送 ReConnect 信号"]
    KC_WAIT --> KC_SELECT
    KC_RES -- 其他错误 --> KC_BREAKER
```

### AsyncProducer.check()

```mermaid
graph TD
    CK_START["进入外层无限循环"] --> CK_STATUS{"Status?"}
    CK_STATUS -- Disconnected --> CK_SLEEP["sleep 5s, continue"]
    CK_STATUS -- Closed --> CK_EXIT["return 退出"]
    CK_STATUS -- Connected --> CK_SELECT["select 监听"]
    CK_SELECT --> CK_SUCCESS["Successes 通道"]
    CK_SELECT --> CK_ERROR["Errors 通道"]
    CK_SELECT --> CK_SIG["signals: 系统信号"]
    CK_SUCCESS --> CK_LOG["log: 消息发送成功"]
    CK_ERROR --> CK_ERR_TYPE{"错误类型?"}
    CK_ERR_TYPE -- OutOfBrokers/NotConnected --> CK_DIS["Status = Disconnected, 发送 ReConnect"]
    CK_ERR_TYPE -- 其他 --> CK_ERRLOG["log: 异步发送失败"]
    CK_SIG --> CK_CLOSE["Status = Closed, return"]
```

> **注意：AsyncProducer.check() 承担了三重职责：信号监听 + 异步发送结果收集 + 断连检测。**

---

## 四、方法对比分析

### 4.1 keepConnect 对比

| 维度 | SyncProducer.keepConnect | AsyncProducer.keepConnect |
|------|--------------------------|---------------------------|
| 系统信号监听 | ✅ `signal.Notify(signals, ...)` | ✅ 完全相同 |
| 退出条件 | `Status == Closed` | `Status == Closed` |
| 重连逻辑 | `breaker.Run` → `NewSyncProducer` | `breaker.Run` → `NewAsyncProducer` |
| 重连成功赋值 | `SyncProducer = &producer` | `AsyncProducer = &producer` |
| BreakerOpen 等待 | 2s | 2s |
| 代码行数 | ~55 行 | ~55 行 |
| **差异点** | 仅在于**创建函数和赋值目标不同** | 仅在于**创建函数和赋值目标不同** |

**结论：两个 `keepConnect` 逻辑 95% 相同，具备抽取条件。**

### 4.2 check 对比

| 维度 | SyncProducer.check | AsyncProducer.check |
|------|--------------------|-----------------------|
| 系统信号监听 | ✅ 唯一职责 | ✅ 职责之一 |
| Successes 通道监听 | ❌ 无 | ✅ 有 |
| Errors 通道监听 | ❌ 无 | ✅ 有（含断连检测） |
| 断开重连触发 | ❌ 无 | ✅ 有 |
| 外层 Disconnected sleep | ❌ 无 | ✅ 有 |
| 代码行数 | ~20 行 | ~45 行 |
| **本质** | **纯粹的信号监听器** | **信号监听 + 异步结果收集 + 断连检测** |

**结论：SyncProducer.check() 完全多余（与 keepConnect 信号监听重复）；AsyncProducer.check() 有独立价值但混入了信号监听。**

---

## 五、现有问题总结

### 问题 1：keepConnect 高度重复

两个 `keepConnect` 唯一差异是：
- 创建连接的函数不同（`NewSyncProducer` vs `NewAsyncProducer`）
- 赋值的目标字段不同（`SyncProducer` vs `AsyncProducer`）

完全可以通过**策略模式**（传入 connect 函数）抽成一个公共方法。

### 问题 2：check 职责不对等

- **SyncProducer.check** 只做信号监听 → 和 `keepConnect` 里的信号监听**完全重复**
- **AsyncProducer.check** 做信号监听 + 异步结果收集 + 断连检测 → 有独立存在的价值，但混入了信号监听

### 问题 3：信号通道重复创建（潜在 Bug）

`keepConnect` 和 `check` 各自创建了独立的 `signals` channel 并调用 `signal.Notify`，同一个信号被两个 goroutine 竞争消费，存在**信号丢失风险**：

```go
// keepConnect 里创建了一个 signals channel
signals := make(chan os.Signal, 1)
signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

// check 里又创建了一个 signals channel
signals := make(chan os.Signal, 1)
signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
```

Go 的 `signal.Notify` 会将信号**随机分发**到注册的 channel，这意味着：
- 如果信号被 `check` 的 channel 收到，`keepConnect` 不会感知到关闭信号
- 如果信号被 `keepConnect` 的 channel 收到，`check` 不会感知到关闭信号
- 可能导致**只有一个 goroutine 退出，另一个泄漏**

---

## 六、优化方向（初步建议）

```mermaid
graph TD
    A["当前结构"] --> B["SyncProducer"]
    A --> C["AsyncProducer"]
    B --> D["keepConnect - 55行"]
    B --> E["check - 20行（纯信号监听，重复）"]
    C --> F["keepConnect - 55行"]
    C --> G["check - 45行（信号 + 异步结果 + 断连）"]

    A --> H["优化方向"]
    H --> I["baseProducer 公共结构体"]
    I --> J["baseKeepConnect: 传入 connect 函数作为策略"]
    I --> K["信号监听统一到一个地方"]
    I --> L["SyncProducer: 删除 check, 合并信号监听到 baseKeepConnect"]
    I --> M["AsyncProducer: check 精简为纯异步结果收集器, 信号监听上移到 base"]
```

### 6.1 抽取 baseProducer

```go
// baseProducer 公共字段，SyncProducer / AsyncProducer 嵌入使用
type baseProducer struct {
    Name       string
    Hosts      []string
    Config     *sarama.Config
    Status     string
    Breaker    *breaker.Breaker
    ReConnect  chan bool
    StatusLock sync.Mutex
    Log        Logger
    exit       chan struct{}   // 统一退出信号
}
```

### 6.2 keepConnect 模板化

```go
// connectFunc 是连接策略：Sync 和 Async 各自提供
type connectFunc func() error

// baseKeepConnect 公共重连逻辑，通过 connectFunc 消除重复
func (b *baseProducer) baseKeepConnect(connect connectFunc) {
    // 统一的信号监听 + 重连逻辑
    // 只需传入不同的 connect 函数即可
}
```

### 6.3 信号监听统一

```go
// 在 baseProducer 中只注册一次 signal.Notify
// 通过 close(b.exit) 广播退出信号给所有 goroutine
func (b *baseProducer) watchSignals() {
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
    <-signals
    close(b.exit)  // 广播退出
}
```

### 6.4 各子类的职责划分

| 组件 | 职责 |
|------|------|
| `baseProducer` | 信号监听、退出广播、公共 keepConnect 模板 |
| `SyncProducer` | 提供 syncConnect 策略函数，**删除 check** |
| `AsyncProducer` | 提供 asyncConnect 策略函数，check 精简为**纯异步结果收集器** |

### 6.5 优化后的结构

```mermaid
graph TD
    A["baseProducer"] --> B["watchSignals(): 统一信号监听"]
    A --> C["baseKeepConnect(connectFunc): 公共重连"]
    A --> D["exit chan struct{}: 统一退出广播"]

    E["SyncProducer"] --> A
    E --> F["syncConnect(): 策略函数"]
    E --> G["Send / SendMessages / Close"]

    H["AsyncProducer"] --> A
    H --> I["asyncConnect(): 策略函数"]
    H --> J["check(): 纯异步结果收集（无信号监听）"]
    H --> K["Send / Close"]
```

---

## 七、优化收益预估

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| keepConnect 代码行数 | 55 + 55 = 110 行 | ~60 行（公共） + 2 × ~5 行（策略函数） |
| check 方法 | 2 个（含重复信号监听） | 1 个（仅 Async，精简后） |
| signal.Notify 注册次数 | 4 次（2 个 goroutine × 2 个生产者类型） | 1 次（baseProducer 统一） |
| 信号丢失风险 | 有（多 channel 竞争） | 无（单 channel + close 广播） |
| 新增生产者类型的成本 | 复制粘贴 ~100 行 | 提供 connect 策略函数 ~5 行 |

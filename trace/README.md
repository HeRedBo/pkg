# trace — 请求上下文数据收集器

## 1. 包的定位与本质

**本包不是分布式链路追踪系统，也不是日志库。**

trace 是一个**请求级数据收集器**——将一次 HTTP 请求生命周期内涉及的各类操作（SQL 执行、缓存操作、第三方服务调用、调试信息）聚合到一个结构体中，提供 JSON 序列化能力。trace 包通过 `logx.Logger` 接口具备日志输出能力，不直接依赖 zap，而是通过 `logx` 接口实现日志解耦——默认通过 logx 的三级优先级机制（`SetLogger()` 注入 > 全局 `logx.GetLogger()` > 默认控制台）回退到控制台输出。

适用场景：服务间通过 HTTP 调用的微服务架构中，作为**请求级别的数据收集工具**，在单条日志中完整记录一次请求的所有关键操作。

### 架构定位：轻量适配器

trace 包负责请求上下文的数据收集、聚合与 JSON 序列化，同时通过 `logx.Logger` 接口提供日志输出能力，默认走 logx 的控制台输出。调用方也可通过 `SetLogger()` 注入自定义日志实现：

```
┌─────────────────────────────────────────────────────┐
│                    业务代码                          │
│  trace.AppendSQL() / AppendCache() / AppendDialog() │
│  trace.SetLogger(customLogger)  // 可选             │
└──────────────────────┬──────────────────────────────┘
                       │ 请求上下文数据
                       ▼
┌─────────────────────────────────────────────────────┐
│              trace（数据收集层 + 日志能力）            │
│  聚合 SQL / Cache / Dialog / Debug → JSON 序列化     │
│  通过 logx.Logger 接口输出日志                       │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
┌──────────────────┐   ┌──────────────────────────┐
│  logx 三级优先级   │   │  调用方自行序列化          │
│  ① SetLogger 注入 │   │  json.Marshal → 自行输出  │
│  ② logx.GetLogger │   └──────────────────────────┘
│  ③ 默认控制台输出  │
└──────────────────┘
```

- **trace**：数据收集器 + 通过 `logx.Logger` 接口具备日志输出能力，不直接依赖 zap
- **logx 三级优先级**：`SetLogger()` 注入 > 全局 `logx.GetLogger()` > 默认控制台（`zap.NewDevelopmentConfig()` 输出到 stdout）
- 调用方可通过 `SetLogger()` 替换为任意日志实现，不设置时默认走 logx 控制台输出

## 2. 核心数据结构

### Trace

核心聚合结构体，包含一次请求的全生命周期信息：

| 字段 | JSON Key | 类型 | 说明 |
|------|----------|------|------|
| `Identifier` | `trace_id` | `string` | 链路 ID，为空时由 `crypto/rand` 生成 20 位随机 hex |
| `Request` | `request` | `*Request` | 入站请求元数据 |
| `Response` | `response` | `*Response` | 响应元数据 |
| `ThirdPartyRequests` | `third_party_requests` | `[]*Dialog` | 第三方服务调用记录 |
| `Debugs` | `debugs` | `[]*Debug` | 自定义调试信息 |
| `SQLs` | `sqls` | `[]*SQL` | SQL 执行记录 |
| `Cache` | `Cache` | `[]*Cache` | 缓存操作记录 |
| `Success` | `success` | `bool` | 请求结果 |
| `CostMillisecond` | `cost_millisecond` | `float64` | 总耗时（ms） |

> **`Logger` 字段（架构改进）**：原始 phper95 设计中 `Logger` 为 `*zap.Logger` 硬依赖，当前版本已替换为 `logx.Logger` 接口抽象，使 trace 包不再强依赖 zap，可替换为任意日志实现。配合 `SetLogger()` 方法使用，不设置时默认走 logx 三级优先级回退到控制台输出。
>
> **`AlwaysTrace` 字段（遗留）**：`bool` 类型，在 trace 包内部没有任何逻辑消费它（无 `if` 判断），仅作为数据字段存在。对应的 `SetAlwaysTrace()` 方法存在 bug：忽略参数 `b`，始终设为 `true`。
>
> **子结构体的 `Logger` / `AlwaysTrace` 字段（遗留）**：每个子结构体（Request、Response、SQL、Cache、Dialog、Debug）上都携带了 `Logger` 和 `AlwaysTrace` 字段，这些是从 phper95 原样保留的。实际上只有 Trace 级别的 Logger 有意义，子结构体上的这两个字段无实际消费方。

所有 `Append*` 方法均通过 `sync.Mutex` 保证并发安全。

### Request

入站请求元数据：

| 字段 | JSON Key | 类型 | 说明 |
|------|----------|------|------|
| `TTL` | `ttl` | `string` | 请求超时时间 |
| `Method` | `method` | `string` | 请求方式（GET/POST 等） |
| `DecodedURL` | `decoded_url` | `string` | 请求地址 |
| `Header` | `header` | `interface{}` | 请求 Header |
| `Body` | `body` | `interface{}` | 请求 Body |

### Response

响应元数据：

| 字段 | JSON Key | 类型 | 说明 |
|------|----------|------|------|
| `Header` | `header` | `interface{}` | 响应 Header |
| `Body` | `body` | `interface{}` | 响应 Body |
| `BusinessCode` | `business_code` | `int` | 业务码（零值省略） |
| `BusinessCodeMsg` | `business_code_msg` | `string` | 业务码提示（零值省略） |
| `HttpCode` | `http_code` | `int` | HTTP 状态码 |
| `HttpCodeMsg` | `http_code_msg` | `string` | HTTP 状态码描述 |
| `CostMillisecond` | `cost_millisecond` | `int64` | 执行耗时（ms） |

### Dialog

第三方服务调用会话记录。支持重试场景下的多次 Response（通过 `AppendResponse` 并发安全追加）。

| 字段 | JSON Key | 类型 | 说明 |
|------|----------|------|------|
| `Request` | `request` | `*Request` | 出站请求信息 |
| `Responses` | `responses` | `[]*Response` | 响应列表（重试时会有多条） |
| `Success` | `success` | `bool` | 是否最终成功 |
| `CostMillisecond` | `cost_millisecond` | `int64` | 总耗时（ms） |

### SQL

单条 SQL 执行细节：

| 字段 | JSON Key | 类型 | 说明 |
|------|----------|------|------|
| `TraceTime` | `trace_time` | `string` | 执行时间，格式 `2006-01-02 15:04:05` |
| `Stack` | `stack` | `string` | 调用位置（文件路径:行号） |
| `SQL` | `sql` | `string` | SQL 语句 |
| `AffectedRows` | `affected_rows` | `int64` | 影响行数 |
| `CostMillisecond` | `cost_millisecond` | `int64` | 执行耗时（ms） |
| `SlowLoggerMillisecond` | `slow_logger_millisecond` | `int64` | 慢查阈值（ms） |

### Cache

单次缓存操作：

| 字段 | JSON Key | 类型 | 说明 |
|------|----------|------|------|
| `Name` | `name` | `string` | 缓存组件名（如 Redis） |
| `TraceTime` | `trace_time` | `string` | 操作时间 |
| `CMD` | `cmd` | `string` | 操作命令（GET/SET 等） |
| `Key` | `key` | `string` | 缓存 Key |
| `Value` | `value` | `interface{}` | 缓存 Value（零值省略） |
| `TTL` | `ttl` | `float64` | 超时时长，单位分（零值省略） |
| `CostMillisecond` | `cost_millisecond` | `int64` | 执行耗时（ms） |
| `SlowLoggerMillisecond` | `slow_logger_millisecond` | `int64` | 慢查阈值（ms） |

### Debug

自定义调试键值对：

| 字段 | JSON Key | 类型 | 说明 |
|------|----------|------|------|
| `Key` | `key` | `string` | 标识 |
| `Value` | `value` | `interface{}` | 值 |
| `CostMillisecond` | `cost_millisecond` | `int64` | 执行耗时（ms） |

### 接口定义

包内定义了两个接口，用于面向接口的编程场景：

- **`T`**：Trace 的接口抽象，核心方法包含 `ID()`、`WithRequest()`、`WithResponse()`、`AppendDialog()`、`AppendSQL()`、`AppendCache()`。`SetLogger()` 用于注入自定义 Logger（不设置时默认走 logx 控制台）。`SetAlwaysTrace()` 存在 bug（忽略参数，始终设为 `true`），且 `AlwaysTrace` 字段在包内部无消费逻辑。
- **`D`**：Dialog 的接口抽象，仅包含 `AppendResponse()` 方法。

### 常量

- `Header`：值为 `"TRACE-ID"`，用于 HTTP Header 中传递链路 ID 的 key 约定。

## 3. 使用方式

trace 的使用模式非常简单：**收集数据 → 序列化为 JSON → 通过 logx.Logger 输出**。trace 通过 `logx.Logger` 接口支持日志输出，默认走控制台。

```go
// 1. 请求进入时创建实例（id 为空则自动生成 20 位随机 hex）
tr := trace.New(requestID)

// 2. 处理过程中追加操作记录（纯数据收集）
tr.AppendSQL(&trace.SQL{
    TraceTime:             time.Now().Format("2006-01-02 15:04:05"),
    SQL:                   "SELECT * FROM users WHERE id = ?",
    AffectedRows:          1,
    CostMillisecond:       12,
    SlowLoggerMillisecond: 100,
    Stack:                 "user_repo.go:42",
})

tr.AppendCache(&trace.Cache{
    Name:            "Redis",
    CMD:             "GET",
    Key:             "user:123",
    CostMillisecond: 2,
})

// 第三方调用：构建 Dialog 并追加响应（支持重试场景多次追加）
dialog := &trace.Dialog{
    Request: &trace.Request{Method: "POST", DecodedURL: "https://api.example.com/order"},
}
dialog.AppendResponse(&trace.Response{HttpCode: 200})
tr.AppendDialog(dialog)

tr.AppendDebug(&trace.Debug{Key: "userId", Value: 123})

// 3. 请求结束时设置请求响应信息
tr.WithRequest(&trace.Request{
    Method:     "POST",
    DecodedURL: "/api/v1/order",
})
tr.WithResponse(&trace.Response{
    HttpCode:        200,
    BusinessCode:    0,
    CostMillisecond: 58,
})

// 4. 数据收集完毕，序列化为 JSON
data, _ := json.Marshal(tr)
//    trace 通过 logx.Logger 接口支持日志输出，默认走控制台
logx.GetLogger().Info("request trace", logx.Field("trace", string(data)))
```

所有 `Append*` 和 `With*` 方法均返回 `*Trace`，支持链式调用。

### 日志输出方式

trace 包通过 `logx.Logger` 接口支持日志输出，默认走 logx 的三级优先级机制回退到控制台输出。调用方可通过 `SetLogger()` 注入自定义日志实现：

```go
// 方式一：使用默认控制台输出（零配置）
// 不设置 Logger 时，trace 内部通过 logx 默认控制台输出
data, _ := json.Marshal(tr)
logx.GetLogger().Info("request trace", logx.Field("trace", string(data)))

// 方式二：注入自定义 Logger
tr.SetLogger(myCustomLogger)
```

**logx 三级优先级**：`SetLogger()` 注入 > 全局 `logx.SetLogger()` / `logx.GetLogger()` > 默认控制台（`zap.NewDevelopmentConfig()` 输出到 stdout）。

caller 可以直接使用 `logx` 包的 API（`logx.GetLogger()`、`logx.Field()` 等），也可以通过 trace 包的转发符号（`trace.Field()`、`trace.ErrField()`）访问日志能力。

> **`logger.go` 文件说明**：该文件包含有意设计的类型别名、优先级回退机制和便捷转发：
>
> | 符号 | 性质 | 说明 |
> |------|------|------|
> | `Logger` 类型别名 | 便捷转发（有意设计） | 等价于 `logx.Logger`，使 trace 包使用者可直接通过 trace 包访问日志能力 |
> | `getLogger()` | 优先级回退机制（有意设计） | 三级优先级：Option 注入（`SetLogger()`）> 全局 `logx.GetLogger()` > 默认控制台输出 |
> | `LogField` 类型别名 | 便捷转发 | 等价于 `logx.LogField`，caller 可直接使用 `logx` 包 |
> | `Field` 变量 | 便捷转发 | 等价于 `logx.Field`，caller 可直接使用 `logx` 包 |
> | `ErrField` 变量 | 便捷转发 | 等价于 `logx.ErrField`，caller 可直接使用 `logx` 包 |

## 4. 与分布式链路追踪（OpenTelemetry）的区别

| 维度 | 本 trace 包 | OpenTelemetry |
|------|------------|---------------|
| 数据模型 | 扁平结构体，单 Trace 聚合所有信息 | Span 树（DAG），支持嵌套和跨服务 |
| 跨服务追踪 | 不支持，各服务独立生成 trace | 核心能力，通过 Context 自动传播 TraceID/SpanID |
| 可视化 | 只能看日志，人工关联 | Jaeger/Tempo 瀑布图、火焰图 |
| 插桩方式 | 手动调用 `Append*` 方法 | 大量自动插桩（HTTP/gRPC/DB/Cache） |
| 采样 | 无采样机制 | 概率采样、尾部采样、自适应采样 |
| 生态集成 | 自定义 JSON 格式 | W3C TraceContext / OTLP 标准，数百个库原生支持 |
| ID 传播 | 无标准传播机制，需自行通过 Header 传递 | W3C `traceparent`/`tracestate` 标准传播 |

## 5. 适用场景与局限性

### 适用场景

- 单体应用或小型微服务项目，快速获得请求级日志聚合能力
- 需要在**单条日志**中聚合请求全生命周期信息（SQL + Cache + 第三方调用 + 调试信息）
- 服务间通过 HTTP 调用，通过 `TRACE-ID` Header 做请求级别的日志关联
- 不想引入 OTel SDK 额外依赖的轻量场景

### 局限性

- **无法跨服务自动追踪完整链路**——每个服务独立生成 trace，无父子 Span 关系传播
- **无可视化**——排查问题依赖日志搜索和人工关联
- **无采样能力**——高并发下全量记录会产生大量日志，无概率/自适应采样
- **手动插桩**——所有操作需要手动调用 `Append*` 方法，容易遗漏
- **无标准传播协议**——不兼容 W3C TraceContext，与其他可观测性系统对接困难

## 6. 演进建议

1. **新项目直接使用 OpenTelemetry**：随着 Go 版本升高和 OTel 成为 Go 生态事实标准（ThoughtWorks 技术雷达 Adopt 级别），新项目建议直接使用 OpenTelemetry Go SDK，获得跨服务追踪、自动插桩、采样、标准化导出等完整能力。

2. **现有项目渐进演进**：保留此包做单服务日志聚合，同时引入 OTel 实现跨服务追踪。两套方案可并行运行。

3. **缓存操作自动接入 OTel**：go-redis v9 的 `redis` 包已具备 `ctx` 参数，推荐使用 `redisotel` Hook 机制自动接入 OpenTelemetry，无需手动记录缓存操作。

4. **长期迁移方向**：将现有数据结构（`SQL`、`Cache`、`Dialog`、`Debug`）迁移为 OTel Span Attributes + Events，统一可观测性方案（Traces + Metrics + Logs）。

## 7. 与参考项目的关系

本包源自 [phper95/pkg](https://github.com/phper95/pkg) 的 trace 包。**原始版本是一个完整的请求日志解决方案**——内置 `*zap.Logger` 硬编码，数据收集与日志输出耦合在一起，trace 自身就能完成从数据聚合到日志输出的全流程。

当前版本在 phper95 基础上进行了**有价值的架构改造**：将 `*zap.Logger` 硬依赖替换为 `logx.Logger` 接口抽象，并通过 logx 三级优先级机制实现日志解耦。trace 不再强依赖 zap，可替换为任意日志实现，零配置时默认走控制台输出：

| 维度 | phper95/pkg 原始 trace | 当前 trace |
|------|----------------------|------------|
| 职责范围 | 数据收集 + 日志输出（一体化） | 数据收集 + JSON 序列化 + 通过 logx.Logger 接口日志输出 |
| 日志依赖 | 硬编码 `*zap.Logger` | logx.Logger 接口（解耦改造） |
| 日志引擎 | zap 硬编码 | logx 三级优先级（默认控制台） |
| 测试覆盖 | 无单元测试 | 补充了完整的单元测试，覆盖并发安全、ID 生成、链式调用等场景 |
| 可替换性 | 无法替换日志实现 | 通过 logx.Logger 接口可替换为任意日志实现 |

简言之：**原始 trace = 数据收集 + zap 硬编码日志输出**（一体化），**当前 trace = 数据收集 + JSON 序列化 + logx.Logger 接口日志输出**（接口解耦）。trace 通过 `logx.Logger` 接口具备日志能力，不直接依赖 zap，零配置时默认走控制台。

### 架构改进说明

以下改造体现了 trace 包在 phper95 基础上的架构价值：

| 改进项 | 原始设计 | 改造后 |
|--------|---------|--------|
| `Trace.Logger` 字段 | `*zap.Logger` 硬依赖 | `logx.Logger` 接口，支持日志输出，可替换为任意实现 |
| `SetLogger()` 方法 | 注入 zap Logger | 注入自定义 `logx.Logger`，不设置时默认走 logx 控制台 |
| `logger.go` 中的 `getLogger()` | 简单获取 Logger | 有意设计的三级优先级回退机制：Option 注入 > 全局 `logx.GetLogger()` > 默认控制台 |
| `Logger` / `Field` / `ErrField` 类型别名 | 无 | 有意设计的便捷转发，使 trace 包使用者可直接通过 trace 包访问日志能力 |

### 遗留代码说明

以下代码是原始 phper95 设计的遗留，在当前架构下已无实际作用或存在问题，保留仅为向后兼容：

| 遗留项 | 原始用途 | 当前状态 |
|--------|---------|----------|
| `Trace.AlwaysTrace` 字段 | 控制是否始终记录 trace | 无消费方（包内部无任何 `if` 判断消费该字段），不影响任何逻辑 |
| `SetAlwaysTrace()` 方法 | 设置 AlwaysTrace 标志 | **存在 bug**：忽略参数 `b`，始终设为 `true`；且字段本身无消费方 |
| 各子结构体的 `Logger` / `AlwaysTrace` 字段 | 每个子结构体都携带了这两个字段 | 遗留（无消费方），只有 Trace 级别的 Logger 有意义 |

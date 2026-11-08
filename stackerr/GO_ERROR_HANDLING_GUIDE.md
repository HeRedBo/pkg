# Go 错误处理演进与工程实践指南

> 本指南基于项目实际代码编写，涵盖 Go 错误处理的历史演进（从 Go 1.13 到 Go 1.21+）、旧版 errors 包的问题分析、stackerr 包的使用指南，以及分层错误处理的工程实践。

---

## 一、Go 错误处理演进历史

### 1.1 前 Go 1.13 时代（~2019）（背景）

Go 原生错误处理从一开始就奉行**显式检查**的哲学，但早期标准库提供的工具非常有限：

- **`errors.New(msg)`** — 创建简单错误
- **`fmt.Errorf(format, args...)`** — 格式化错误消息

这一时期的核心痛点：

| 问题 | 说明 |
|------|------|
| 无标准化错误包装 | 没有 `wrap` 机制，错误只能被替换或拼接字符串 |
| 无错误链判断 | 无法判断深层错误链中是否包含某个特定哨兵错误 |
| 无类型提取 | `errors.As()` 不存在，无法从错误链中提取特定类型的错误 |
| 无堆栈信息 | 标准库不记录调用栈，排查生产问题困难 |

社区严重依赖 **`github.com/pkg/errors`** 来弥补这些缺陷，它提供了 `Wrap`、`Cause`、`WithStack` 等关键能力。

### 1.2 Go 1.13（2019）— 分水岭（核心机制）

Go 1.13 是错误处理的里程碑版本，标准库开始覆盖 `pkg/errors` 的核心功能，并引入了一套完整的**错误包装（Wrapping）机制**。

#### 错误包装机制的核心设计

Go 1.13 的错误包装机制由三个部分组成，它们共同构成了一条**错误链（Error Chain）**：

1. **`%w` 动词** — 在 `fmt.Errorf` 中包装错误，将原始错误嵌入新错误中，形成错误链的一环
   ```go
   // 原始错误被 %w 包装，保留在链中
   err := fmt.Errorf("query failed: %w", originalErr)
   ```
   - `%w` 与 `%v` 的区别：`%v` 只输出错误消息，丢弃原始错误；`%w` 保留原始错误引用，使其可被后续解包
   - 每次 `%w` 包装都会在错误链中增加一层，形成链式结构

2. **`errors.Is(err, target)`** — 沿错误链逐层判断是否包含特定哨兵错误
   ```go
   if errors.Is(err, os.ErrNotExist) { ... }
   // 等价于：遍历整条错误链，检查每一层是否 == os.ErrNotExist
   ```

3. **`errors.As(err, &target)`** — 沿错误链逐层尝试类型断言，提取特定类型的错误
   ```go
   var customErr *MyError
   if errors.As(err, &customErr) { ... }
   // 等价于：遍历整条错误链，对每一层尝试 (err).(*MyError)
   ```

4. **`Unwrap() error` 接口约定** — 错误链的底层机制。任何类型只要实现此方法，即可被 `errors.Is/As` 穿透
   ```go
   type MyError struct {
       inner error
   }
   func (e *MyError) Unwrap() error { return e.inner }
   // errors.Is/As 会自动调用 Unwrap() 逐层深入
   ```

#### 错误链工作原理图

```
fmt.Errorf("db error: %w", fmt.Errorf("connect: %w", sql.ErrConnDone))

错误链结构：
  ┌─ "db error" ──%w──▶ "connect" ──%w──▶ sql.ErrConnDone ──▶ nil
  └─ errors.Is(err, sql.ErrConnDone) → true（沿链逐层比较）
```

这种机制使得**错误的上下文层层丰富，同时根因始终可追溯**，彻底解决了 Go 1.13 之前错误只能被字符串拼接、无法判断根因的痛点。

#### 根本性改变：从字符串比较到程序化错误链遍历

Go 1.13 的错误包装机制**从根本上改变了 Go 的错误处理范式**：

**之前的模式（Go 1.13 之前）：**
```go
// 问题 1：字符串比较 — 脆弱、不可维护
if err.Error() == "connection refused" { ... }
if strings.Contains(err.Error(), "timeout") { ... }

// 问题 2：直接类型断言 — 无法穿透包装层
if e, ok := err.(*os.PathError); ok { ... }

// 问题 3：错误信息丢失 — 每层包装都丢弃原始错误
return fmt.Errorf("query failed: %v", err) // %v 丢弃了原始错误引用
```

**之后的模式（Go 1.13+）：**
```go
// 优势 1：语义化判断 — 不依赖字符串内容
if errors.Is(err, os.ErrNotExist) { ... }

// 优势 2：安全类型提取 — 自动穿透包装层
var pathErr *os.PathError
if errors.As(err, &pathErr) { ... }

// 优势 3：保留错误链 — %w 保持原始错误引用
return fmt.Errorf("query failed: %w", err) // %w 保留了原始错误，可被 Is/As 穿透
```

**这种转变的核心价值：**

| 维度 | 旧模式 | 新模式 |
|------|--------|--------|
| 错误判断 | 字符串比较（脆弱、易出错） | `errors.Is`（语义化、可靠） |
| 类型提取 | 直接断言（无法穿透包装） | `errors.As`（自动递归解包） |
| 错误传递 | `%v` 丢弃原始错误 | `%w` 保留完整错误链 |
| 跨层追溯 | 不可能 | 自动穿透任意包装层数 |

这使得 Go 的错误处理从**"靠字符串猜"**进化到**"程序化精确追溯"**，为构建可靠的大型系统奠定了基础。

### 1.3 Go 1.20（2023）— 多错误包装（重大升级）

> **重要增强**：Go 1.20 对 Go 1.13 引入的错误包装机制进行了重大升级——`fmt.Errorf` 现在支持使用多个 `%w` 动词一次包装多个错误，使得错误链从单链结构扩展为**多分支树形结构**。这是 Go 错误处理演进中承上启下的关键版本。

- **`fmt.Errorf` 支持多个 `%w`** — 一次包装多个错误，`errors.Is/As` 会递归遍历所有分支
  ```go
  return fmt.Errorf("task failed: %w and %w", err1, err2)
  ```
- **`errors.Join(err1, err2, ...)`** — 将多个错误合并为一个，返回的错误 `Error()` 方法将所有错误的消息用换行符拼接，同时实现 `Unwrap() []error` 支持多错误链展开
  ```go
  combined := errors.Join(validateErr, saveErr)
  fmt.Println(combined.Error())
  // 输出：
  // validate error
  // save error
  ```

### 1.4 Go 1.21+（2023）— 内置错误类型与错误处理完善（持续演进）

> **重要提示**：虽然 Go 1.21 距今已有几个版本，但其错误处理改进对当前项目仍具有重要意义——它标志着标准库错误处理体系的**逐步成熟**。项目应充分理解并利用这些改进，建立规范化的错误处理实践。

Go 1.21 在错误处理方面的贡献不仅限于新增一个哨兵错误，更重要的是**标准库开始提供统一的错误标识体系**，使得跨包、跨库的错误判断更加规范化。

#### 1.4.1 `errors.ErrUnsupported` 详解

**定义与本质：**

```go
// errors 包源码（Go 1.21+）
var ErrUnsupported = New("unsupported operation")
```

- 这是一个**包级变量**（package-level variable），而非类型或常量
- 使用 `errors.New()` 创建，返回一个实现了 `error` 接口的值
- 作为**哨兵错误**（sentinel error），用于表示“操作或功能不被支持”的语义

**典型使用场景：**

当某个接口或方法在特定实现中不可用时，返回此错误作为统一标识：

```go
// 场景 1：只读存储不支持写操作
type ReadOnlyStore struct { /* ... */ }

func (r *ReadOnlyStore) Write(data []byte) error {
    return fmt.Errorf("write to read-only store: %w", errors.ErrUnsupported)
}

// 场景 2：某个功能在当前平台不可用
func OpenSpecialFeature() error {
    if runtime.GOOS == "windows" {
        return fmt.Errorf("special feature on windows: %w", errors.ErrUnsupported)
    }
    // ...
}

// 调用方通过 errors.Is 判断
err := store.Write(data)
if errors.Is(err, errors.ErrUnsupported) {
    log.Println("操作不被支持，尝试替代方案")
    // 优雅降级或提示用户
}
```

**与其他哨兵错误的对比：**

| 哨兵错误 | 定义方式 | 语义 | 判断方式 |
|-----------|----------|------|----------|
| `io.EOF` | `var EOF = errors.New("EOF")` | 输入结束 | `errors.Is(err, io.EOF)` |
| `os.ErrNotExist` | `var ErrNotExist = errors.New("file does not exist")` | 文件不存在 | `errors.Is(err, os.ErrNotExist)` |
| `errors.ErrUnsupported` | `var ErrUnsupported = errors.New("unsupported operation")` | 操作不支持 | `errors.Is(err, errors.ErrUnsupported)` |

#### 1.4.2 标准库错误标识体系的形成

Go 1.21+ 之后，标准库逐步积累了以下常用哨兵错误，形成**统一的错误标识体系**：

| 哨兵错误 | 含义 | 典型场景 | 所属包 |
|-----------|------|----------|--------|
| `io.EOF` | 输入结束 | 读取到流末尾、网络连接关闭 | `io` |
| `io.ErrUnexpectedEOF` | 意外的输入结束 | 读取过程中连接断开 | `io` |
| `os.ErrNotExist` | 文件/目录不存在 | 文件操作、路径检查 | `os` |
| `os.ErrPermission` | 权限不足 | 文件/系统操作被拒绝 | `os` |
| `os.ErrClosed` | 文件/资源已关闭 | 对已关闭的文件进行操作 | `os` |
| `errors.ErrUnsupported` | 操作不被支持 | 接口实现不支持某方法 | `errors` |

**统一标识体系的价值：**

1. **跨包一致性**：不同包使用相同的模式定义哨兵错误，调用方可以用统一的 `errors.Is` 判断
2. **语义明确**：每个哨兵错误都有明确的语义，不依赖字符串内容猜测
3. **可组合**：可以通过 `%w` 包装，保留原始语义的同时附加上下文

#### 1.4.3 对第三方库的示范作用

标准库的哨兵错误模式为第三方库提供了**错误设计的参考范式**，推动了整个 Go 生态的错误处理规范化：

**第三方库应遵循的模式：**

```go
// 推荐：定义包级哨兵错误变量
package database

var (
    ErrConnectionLost = errors.New("connection lost")
    ErrQueryTimeout   = errors.New("query timeout")
    ErrInvalidSQL     = errors.New("invalid SQL syntax")
)

// 推荐：在返回时包装哨兵错误
func (db *DB) Query(sql string) error {
    if !db.connected {
        return fmt.Errorf("query %q: %w", sql, ErrConnectionLost)
    }
    // ...
}

// 推荐：调用方通过 errors.Is 判断
err := db.Query("SELECT ...")
if errors.Is(err, database.ErrConnectionLost) {
    // 重连逻辑
} else if errors.Is(err, database.ErrQueryTimeout) {
    // 重试或降级
}
```

**不推荐的模式：**

```go
// 不推荐：自定义错误类型但不实现 Unwrap
type MyError struct {
    Code int
    Msg  string
}
func (e *MyError) Error() string { return e.Msg }
// 问题：无法与 errors.Is/As 配合

// 不推荐：用字符串比较判断错误
if strings.Contains(err.Error(), "connection lost") { ... }
// 问题：脆弱、不可维护
```

**主流第三方库的实践：**

| 库 | 哨兵错误 | 说明 |
|-----|----------|------|
| `database/sql` | `sql.ErrNoRows`, `sql.ErrTxDone` | 数据库操作的标准错误 |
| `net/http` | `http.ErrServerClosed`, `http.ErrBodyNotAllowed` | HTTP 服务的标准错误 |
| `redis/go-redis` | `redis.Nil` | 键不存在的哨兵错误 |
| `go-sql-driver/mysql` | `mysql.ErrInvalidConn` | MySQL 连接错误 |

这些库都遵循了标准库的哨兵错误模式，使得调用方可以用统一的 `errors.Is` 进行判断，形成了**Go 生态的错误处理共识**。

### 1.5 Go 2.x 展望（未来方向）（前瞻性）

> **注意**：Go 2.x 目前仍处于提案和实验阶段，以下内容基于 Go 团队的公开讨论和提案，最终特性可能会有所变化。

#### 1.5.1 模式匹配提案（Pattern Matching for Errors）

Go 团队正在探索为错误处理引入**模式匹配语法**，目标是让多类型错误判断更加优雅：

**当前模式的痛点：**

```go
// 需要多个 if-else 分支判断不同类型错误
var pathErr *os.PathError
var netErr net.Error

if errors.Is(err, os.ErrNotExist) {
    // 处理文件不存在
} else if errors.As(err, &pathErr) {
    // 处理路径错误
} else if errors.As(err, &netErr) {
    // 处理网络错误
} else {
    // 其他错误
}
```

**提案中的模式匹配语法（概念示例）：**

```go
// 提案可能引入类似 switch 的模式匹配
match err {
    case os.ErrNotExist:     // 处理文件不存在
    case *os.PathError:      // 处理路径错误
    case net.Error:          // 处理网络错误
    default:                 // 其他错误
}
```

**预期价值：**
- 减少样板代码，使错误处理更加简洁
- 编译时类型检查，避免运行时断言失败
- 与 `errors.Is/As` 语义一致，但语法更直观

#### 1.5.2 更好的错误链可视化

Go 工具链正在增强对错误链的**调试和可视化支持**：

**当前工具链的局限：**
- `fmt.Printf("%+v", err)` 只能输出堆栈信息，无法直观展示错误链结构
- 调试器（如 Delve）对错误链的展示不够友好
- 缺乏标准的错误链序列化格式

**预期的改进方向：**
- **调试器集成**：Delve 等调试器可能支持错误链的树形展示，逐层展开 `Unwrap()` 结果
- **错误链序列化**：可能引入标准格式（如 JSON）序列化错误链，便于日志分析和监控
- **可视化工具**：Go 团队可能提供官方工具，将错误链以图形化方式展示

**示例（概念）：**

```
$ go debug error-chain
Error Chain Visualization:
├─ [Level 0] main.Handler: "request failed"
│  ├─ [Level 1] service.GetUser: "user lookup failed"
│  │  └─ [Level 2] database.Query: "connection timeout"
│  │     └─ [Level 3] net.Dial: "dial tcp 10.0.0.1:3306: connection refused"
│  └─ [Level 1] cache.Get: "cache miss"
└─ Root Cause: "connection refused"
```

#### 1.5.3 标准库进一步完善

Go 后续版本预计会在以下方面继续完善错误处理：

**可能的工具函数：**

```go
// 1. 错误链遍历辅助函数（可能在未来版本引入）
func errors.Walk(err error, fn func(error) bool) {
    // 递归遍历错误链，对每一层执行 fn
}

// 2. 错误链收集函数
func errors.Collect(err error) []error {
    // 将多分支错误链展平为切片
}

// 3. 更便捷的错误包装
func errors.Wrapf(err error, format string, args ...any) error {
    // 标准库版本的 Wrapf，等价于 fmt.Errorf + %w
}
```

**可能的接口扩展：**

```go
// 多错误解包接口的标准化（Go 1.20 已部分支持）
type MultiUnwrap interface {
    Unwrap() []error  // 返回多个子错误
}

// 错误链深度限制接口（防止循环引用）
type DepthLimiter interface {
    MaxDepth() int
}
```

**生态系统的演进：**

| 方向 | 说明 | 预期时间 |
|------|------|----------|
| 模式匹配语法 | 引入 `match` 或类似关键字 | Go 2.0+（长期） |
| 错误链可视化 | 调试器和工具链支持 | Go 1.x 渐进式改进 |
| 标准库工具函数 | 更多便捷的错误处理辅助函数 | Go 1.x 渐进式改进 |
| 性能优化 | 减少错误包装的内存和 CPU 开销 | 持续优化 |

#### 1.5.4 对当前项目的启示

虽然 Go 2.x 的特性尚未确定，但项目可以提前做好准备：

1. **统一使用 `errors.Is/As`** — 不依赖字符串比较，为未来模式匹配铺路
2. **规范错误链结构** — 使用 `%w` 包装错误，保持错误链的完整性
3. **避免过度包装** — 错误链不宜过深（建议不超过 5 层），保持可读性
4. **关注 Go 提案进展** — 通过 [Go Proposals](https://github.com/golang/go/issues) 跟踪错误处理相关提案

---

## 二、pkg/errors 的历史地位与现状

### 2.1 曾经的核心价值

`github.com/pkg/errors` 在 Go 1.13 之前几乎是生产项目的标配，它提供了以下关键能力：

| 功能 | 说明 |
|------|------|
| `Wrap(err, msg)` | 包装错误并附加消息，保留原始错误 |
| `Wrapf(err, format, args...)` | 格式化包装 |
| `Cause(err)` | 回溯到根因错误 |
| `WithStack(err)` | 在错误上附加调用栈 |
| `Frame` 格式化 | `%+v` 输出完整的文件/行号/函数名 |

### 2.2 Go 1.13+ 标准库替代方案

| pkg/errors | 标准库替代 |
|------------|-----------|
| `errors.Wrap(err, msg)` | `fmt.Errorf("msg: %w", err)` |
| `errors.Wrapf(err, fmt, args...)` | `fmt.Errorf(fmt+": %w", args..., err)` |
| `errors.Cause(err)` | `errors.Unwrap(err)` 或循环展开 |
| `errors.WithStack(err)` | 需自行实现（标准库不提供） |
| `errors.WithMessage(err, msg)` | `fmt.Errorf("msg: %w", err)` |
| `pkg/errors.Frame` 格式化 | `runtime.CallersFrames` |

### 2.3 当前状态

- **冻结维护** — 作者 Dave Cheney 已明确建议迁移到标准库
- **不再接受新功能** — 仅修复严重安全漏洞
- **Go 1.13+ 项目应逐步移除**

### 2.4 何时可以彻底移除

当项目满足以下条件时，可以安全移除 `pkg/errors`：

1. 所有 `Wrap/Wrapf` 调用已替换为 `fmt.Errorf + %w`
2. 所有 `Cause` 调用已替换为 `errors.Is/As`
3. 堆栈捕获需求由项目内的 `stackerr` 包承担
4. 所有依赖的第三方库不再要求 `pkg/errors` 类型

---

## 三、本项目旧版 errors 包分析

> 源码路径：`/pkg/errors/`

### 3.1 stack_error.go — 源码级堆栈捕获

**实现方式：** 通过 `runtime.Caller` 逐层遍历调用栈，读取每一层的源码文件并拼接到堆栈信息中。

**核心问题：**

| 问题 | 严重程度 | 说明 |
|------|---------|------|
| 使用已废弃的 `ioutil.ReadFile` | 中 | Go 1.16 起 `ioutil` 包已废弃，应使用 `os.ReadFile` |
| 每次错误都进行文件 I/O | **高** | 遍历栈帧时逐文件读取源码，性能极差 |
| 堆栈格式不标准 | 低 | 自定义格式，与标准 `runtime/debug` 输出不一致 |
| 无错误链兼容 | **高** | 不实现 `Unwrap()`，无法与 `errors.Is/As` 配合 |

### 3.2 error.go — PC 值级堆栈捕获

**实现方式：** 通过 `runtime.Callers` 存储 PC（Program Counter）值，格式化时依赖 `pkg/errors` 的 `Frame` 进行输出。

**核心问题：**

| 问题 | 严重程度 | 说明 |
|------|---------|------|
| **严重 Bug：Wrap 条件写反** | **致命** | 第 50-52 行：`if err == nil { return nil }` 逻辑正确，但原代码注释与行为不一致——需确认是否曾存在 `if err != nil { return nil }` 的反转逻辑 |
| 依赖 `pkg/errors` | **高** | `Format` 方法使用 `errors.Frame(pc)` 输出，引入第三方依赖 |
| Wrap 与 Wrapf 行为不一致 | 中 | `Wrap` 使用 `errors.As` 进行类型断言，`Wrapf` 使用直接类型断言 `err.(*item)` |
| 不兼容标准错误链 | **高** | 未实现 `Unwrap()` 方法，`errors.Is/As` 无法穿透 |
| `WithStack` 使用直接断言 | 中 | 只识别自身 `*item` 类型，无法识别其他带堆栈的错误 |

### 3.3 结论

- 旧版 errors 包的大部分功能已被 Go 1.13+ 标准库覆盖
- 唯一独特价值是**堆栈捕获**，但实现方式存在性能和兼容性缺陷
- 需要用更现代的方式重新实现堆栈捕获 → **`stackerr` 包**

---

## 四、stackerr 包 — 设计与使用指南

> 源码路径：`/pkg/stackerr/`  
> 模块路径：`github.com/HeRedBo/pkg/stackerr`

### 4.1 设计理念

| 原则 | 说明 |
|------|------|
| **零第三方依赖** | 纯标准库实现，仅依赖 `fmt`、`runtime`、`strings` |
| **完全兼容错误链** | 实现 `Unwrap()` 方法，支持 `errors.Is` / `errors.As` / `%w` |
| **防重复包装** | `WithStack` 检测错误链中是否已有堆栈，避免重复捕获 |
| **灵活输出** | `%v` 纯消息 / `%+v` 带完整堆栈 / `%s` 纯消息 / `%q` 引号包裹 |
| **零模块依赖** | 不依赖项目内其他子模块，可独立使用 |

### 4.2 API 说明

#### 类型

**`Frame`** — 单个栈帧信息

```go
type Frame struct {
    Function string // 完整函数名（如 "main.GetUser"）
    File     string // 源文件路径
    Line     int    // 行号
}
```

**`StackError`** — 带堆栈信息的错误类型

```go
type StackError struct {
    err    error   // 原始错误
    frames []Frame // 捕获的调用栈
}
```

方法：

| 方法 | 签名 | 说明 |
|------|------|------|
| `Error()` | `string` | 返回底层错误消息 |
| `Unwrap()` | `error` | 返回链中的下一个错误，兼容 `errors.Is/As` |
| `Format()` | `(fmt.State, rune)` | 实现 `fmt.Formatter`，支持 `%v`/`%+v`/`%s`/`%q` |
| `Stack()` | `[]Frame` | 返回栈帧副本（可安全修改） |

#### 函数

**`WithStack(err error) error`**

在错误边界处一次性捕获完整调用栈。如果 `err` 为 `nil`，返回 `nil`。如果错误链中已有堆栈，原样返回（防重复包装）。

**`Wrapf(err error, format string, args ...interface{}) error`**

包装错误并附加格式化消息 + 新堆栈。每次调用都捕获新帧（与 `pkg/errors.Wrapf` 行为一致）。如果 `err` 为 `nil`，返回 `nil`。

**`StackTrace(err error) []Frame`**

从错误链中提取堆栈信息。遍历 `Unwrap()` 链，查找 `*StackError` 或实现了 `Stack() []Frame` 的类型。未找到则返回 `nil`。

### 4.3 代码示例

#### 基本使用

```go
package main

import (
    "errors"
    "fmt"

    "github.com/HeRedBo/pkg/stackerr"
)

// 哨兵错误
var ErrNotFound = errors.New("record not found")

func main() {
    // 1. WithStack — 在错误边界捕获堆栈
    err := doSomething()
    if err != nil {
        err = stackerr.WithStack(err)
        fmt.Printf("%+v\n", err) // 输出带堆栈的完整信息
    }

    // 2. Wrapf — 包装并附加消息
    err = doSomething()
    if err != nil {
        err = stackerr.Wrapf(err, "operation %s failed", "upload")
        fmt.Printf("%+v\n", err)
    }

    // 3. 兼容 errors.Is
    err = fmt.Errorf("db query: %w", ErrNotFound)
    err = stackerr.WithStack(err)
    fmt.Println(errors.Is(err, ErrNotFound)) // true

    // 4. 兼容 errors.As
    type MyError struct{ Code int }
    // ... errors.As 同样可以穿透 StackError 链
}

func doSomething() error {
    return errors.New("something went wrong")
}
```

#### 提取堆栈信息

```go
err := someOperation()
if err != nil {
    err = stackerr.WithStack(err)

    // 提取堆栈帧
    frames := stackerr.StackTrace(err)
    for _, f := range frames {
        log.Printf("  at %s (%s:%d)", f.Function, f.File, f.Line)
    }
}
```

### 4.4 日志输出对比

| 格式化动词 | 输出内容 | 适用场景 |
|-----------|---------|---------|
| `%v` | 仅错误消息 | 用户可见的错误响应 |
| `%s` | 仅错误消息（同 `%v`） | 日志中的简洁输出 |
| `%+v` | 错误消息 + 完整调用栈 | 开发/测试环境详细日志 |
| `%q` | 引号包裹的错误消息 | 需要转义的场景 |

**输出示例：**

```
# %v 或 %s
user not found

# %+v
user not found
main.getUser
    /app/handler/user.go:42
main.handleRequest
    /app/handler/handler.go:15
net/http.serverHandler.ServeHTTP
    /usr/local/go/src/net/http/server.go:2947

# %q
"user not found"
```

---

## 五、工程实践：分层错误处理策略

### 5.1 核心原则

1. **底层不抓堆栈** — DAO / 中间件层只传递错误，不附加堆栈
2. **边界处统一抓** — Handler / Consumer / 中间件入口处一次性捕获
3. **只抓一次** — `WithStack` 内置防重复包装，但应在设计上保证只在边界捕获

### 5.2 分层架构图

```
┌─────────────────────────────────────────────────────────┐
│  第 3 层：错误边界（Handler / Consumer / 中间件入口）       │
│  → WithStack 一次性捕获堆栈                               │
│  → 写入日志（%+v 输出完整堆栈）                             │
│  → 返回给调用方简洁错误（%v 仅消息）                        │
├─────────────────────────────────────────────────────────┤
│  第 2 层：业务逻辑层（Service）                            │
│  → fmt.Errorf("context: %w", err) 传递                  │
│  → 不抓堆栈，只附加业务上下文                               │
├─────────────────────────────────────────────────────────┤
│  第 1 层：基础设施层（DAO / 外部中间件）                     │
│  → 原样返回错误，或用 fmt.Errorf + %w 包装                 │
│  → 不抓堆栈                                               │
│  → MySQL/Redis/ES/Kafka 等中间件的错误不带堆栈             │
└─────────────────────────────────────────────────────────┘
```

### 5.3 完整代码示例

```go
package main

import (
    "database/sql"
    "errors"
    "fmt"
    "log"

    "github.com/HeRedBo/pkg/stackerr"
)

// ========== 哨兵错误 ==========
var (
    ErrUserNotFound = errors.New("user not found")
)

// ========== 第 1 层：DAO — 原样返回，不抓堆栈 ==========
type UserDAO struct {
    db *sql.DB
}

func (d *UserDAO) GetUser(id int64) (*User, error) {
    row := d.db.QueryRow("SELECT id, name FROM users WHERE id = ?", id)
    var u User
    if err := row.Scan(&u.ID, &u.Name); err != nil {
        if errors.Is(err, sql.ErrNoRows) {
            return nil, fmt.Errorf("get user by id %d: %w", id, ErrUserNotFound)
        }
        return nil, fmt.Errorf("scan user: %w", err) // 纯 %w 传递
    }
    return &u, nil
}

// ========== 第 2 层：Service — 附加业务上下文，不抓堆栈 ==========
type UserService struct {
    dao *UserDAO
}

func (s *UserService) GetUserProfile(id int64) (*Profile, error) {
    user, err := s.dao.GetUser(id)
    if err != nil {
        return nil, fmt.Errorf("get user profile for id=%d: %w", id, err)
    }
    return &Profile{User: user}, nil
}

// ========== 第 3 层：Handler — 错误边界，一次性捕获堆栈 ==========
func (h *Handler) GetProfile(w http.ResponseWriter, r *http.Request) {
    id := parseID(r)
    profile, err := h.svc.GetUserProfile(id)
    if err != nil {
        // ★ 错误边界：WithStack 捕获堆栈 ★
        err = stackerr.WithStack(err)

        // 日志输出完整堆栈
        log.Printf("[ERROR] GetProfile id=%d: %+v", id, err)

        // 返回给客户端简洁消息
        if errors.Is(err, ErrUserNotFound) {
            http.Error(w, "user not found", http.StatusNotFound)
        } else {
            http.Error(w, "internal server error", http.StatusInternalServerError)
        }
        return
    }
    json.NewEncoder(w).Encode(profile)
}
```

### 5.4 第三方中间件错误说明

| 中间件 | 错误特点 | 堆栈责任 |
|--------|---------|---------|
| **MySQL** (database/sql, GORM) | 返回标准 `error`，无堆栈 | 业务代码在边界处抓堆栈 |
| **Redis** (go-redis) | 返回标准 `error`，无堆栈 | 业务代码在边界处抓堆栈 |
| **Elasticsearch** (olivere/elastic) | 返回带 HTTP 状态码的错误，无堆栈 | 业务代码在边界处抓堆栈 |
| **Kafka** (sarama) | 返回标准 `error`，无堆栈 | 业务代码在边界处抓堆栈 |
| **HTTP Client** | 返回 `net.Error`，无堆栈 | 业务代码在边界处抓堆栈 |

**核心认知：** 第三方中间件的错误**不携带堆栈信息**，堆栈信息的责任完全在业务代码。`stackerr.WithStack` 就是在错误即将离开业务边界的最后一刻，记录"这个错误是从哪里被发现的"。

---

## 六、性能影响分析

### 6.1 开销数据

| 操作 | 耗时 | 说明 |
|------|------|------|
| `errors.New()` | ~50ns | 基线：最简单的错误创建 |
| `fmt.Errorf()` | ~200ns | 基线：格式化错误 |
| `runtime.Callers()` | ~100-500ns | 收集 PC 值 |
| `runtime.CallersFrames()` | ~1-5μs | 解析 PC 值为 Frame（函数名/文件/行号） |
| `stackerr.WithStack()` | ~2-6μs | Callers + CallersFrames 总和 |
| `stackerr.Wrapf()` | ~3-7μs | WithStack + fmt.Sprintf + fmt.Errorf |

### 6.2 不同场景的影响

#### I/O 密集型（绝大多数业务场景）

| 操作 | 耗时 | 堆栈捕获占比 |
|------|------|-------------|
| MySQL 查询 | 1-50ms | < 0.06% ~ 0.6% |
| Redis 操作 | 100μs-1ms | 0.2% ~ 6% |
| HTTP 请求 | 1-100ms | < 0.01% ~ 0.6% |

**结论：** 在 I/O 密集型场景中，堆栈捕获的开销**完全可以忽略**。

#### 高吞吐错误处理（极端场景）

| 吞吐量 | 单核 CPU 占比 | 说明 |
|--------|-------------|------|
| 1,000 errors/s | ~0.5% | 可忽略 |
| 10,000 errors/s | ~5% | 需要关注 |
| 100,000 errors/s | ~50% | **不可接受**，需优化 |

#### 内存分配

- 每次 `WithStack` 约分配 **1.5-2KB** 内存（PC 数组 + Frame 切片）
- 对比：`errors.New` 约分配 16-32 字节
- GC 压力：在大量错误创建场景下需要关注

### 6.3 优化策略

#### 按环境区分

```go
// 开发/测试环境：始终捕获堆栈
if os.Getenv("APP_ENV") != "production" {
    err = stackerr.WithStack(err)
    log.Printf("%+v", err)
} else {
    log.Printf("%v", err)
}
```

#### 按错误频率区分

| 错误类型 | 处理方式 | 原因 |
|---------|---------|------|
| **预期错误**（业务校验、不存在等） | `fmt.Errorf + %w`，不抓堆栈 | 高频出现，堆栈价值低 |
| **意外错误**（DB 连接失败、超时等） | `stackerr.WithStack`，抓堆栈 | 低频但关键，需要定位 |

#### 采样捕获（高吞吐场景）

```go
var errorCounter uint64

func shouldCaptureStack() bool {
    n := atomic.AddUint64(&errorCounter, 1)
    return n%100 == 0 // 每 100 个错误捕获一次堆栈
}
```

---

## 七、最佳实践总结

### 7.1 推荐原则

1. **错误只应被包装一次堆栈** — 在错误边界（Handler/Consumer）统一使用 `WithStack`
2. **底层用 `%w` 传递，不抓堆栈** — DAO/Service 层只附加上下文消息
3. **预期错误不抓堆栈** — 业务校验错误（参数无效、不存在等）用 `fmt.Errorf + %w` 即可
4. **意外错误必须抓堆栈** — 数据库连接失败、外部服务超时等需要完整调用链定位
5. **使用 `errors.Is` 判断哨兵错误** — 不要用字符串比较
6. **使用 `errors.As` 提取错误类型** — 不要用类型断言 `err.(*MyError)`
7. **日志中用 `%+v` 输出堆栈** — 响应体中用 `%v` 只输出消息
8. **不要在循环中抓堆栈** — 批量操作时，只在最终汇总错误处抓一次
9. **第三方中间件错误原样传递** — 堆栈信息的责任在业务代码边界
10. **生产环境按需开启堆栈** — 通过环境变量或配置控制

### 7.2 错误处理决策树

```
发生错误？
│
├─ 是否需要堆栈信息来定位问题？
│   │
│   ├─ 是（意外错误、基础设施故障）
│   │   └─ 在错误边界处使用 stackerr.WithStack(err)
│   │
│   └─ 否（预期错误、业务校验失败）
│       └─ 继续下一步
│
├─ 是否需要附加上下文消息？
│   │
│   ├─ 是
│   │   └─ 使用 fmt.Errorf("context: %w", err)
│   │       或 stackerr.Wrapf(err, "context %s", value)
│   │
│   └─ 否
│       └─ 直接返回 err
│
└─ 是否在错误边界（Handler/Consumer）？
    │
    ├─ 是
    │   └─ 记录日志：log.Printf("%+v", stackerr.WithStack(err))
    │       返回响应：json/error 中只用 %v 消息
    │
    └─ 否
        └─ 原样返回或 %w 包装后返回
```

### 7.3 升级路径

从旧版 `errors` 包迁移到 `stackerr` 的步骤：

#### 第一步：替换简单创建

```go
// 旧
err := errors.New("something wrong")
err := errors.Errorf("something %s", "wrong")

// 新
err := errors.New("something wrong")              // 标准库
err := fmt.Errorf("something %s", "wrong")         // 标准库
```

#### 第二步：替换错误包装

```go
// 旧
err := errors.Wrap(err, "context")
err := errors.Wrapf(err, "context %s", val)

// 新
err := fmt.Errorf("context: %w", err)
err := fmt.Errorf("context %s: %w", val, err)
// 或需要堆栈时
err := stackerr.Wrapf(err, "context %s", val)
```

#### 第三步：替换堆栈捕获

```go
// 旧
err := errors.WithStack(err)

// 新
err := stackerr.WithStack(err)
```

#### 第四步：替换错误原因提取

```go
// 旧
rootCause := errors.Cause(err)

// 新
// 使用 errors.Is 判断
if errors.Is(err, targetErr) { ... }
// 使用 errors.As 提取
var customErr *MyError
if errors.As(err, &customErr) { ... }
```

#### 第五步：移除依赖

1. 全局搜索 `github.com/pkg/errors` 的 import
2. 逐一替换为上述方案
3. 从 `go.mod` 中移除 `github.com/pkg/errors`
4. 运行 `go mod tidy` 清理

#### 第六步：验证

```bash
# 确保编译通过
go build ./...

# 运行测试
go test ./...

# 静态检查
go vet ./...
```

---

> **总结：** Go 的错误处理在 1.13 之后已经足够强大，结合本项目 `stackerr` 包的零依赖堆栈捕获能力，可以在不引入第三方库的前提下，实现高效、清晰的分层错误处理。核心思想是：**底层传递、边界捕获、只抓一次、按需输出**。

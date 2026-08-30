# DB 包日志改造设计文档

## 1. 改造背景

### 1.1 原有设计的问题

改造前，`db` 模块的日志系统存在以下问题：

| 问题 | 说明 |
|------|------|
| stdLogger 硬编码 | 内部直接使用 `gorm/logger.Default`（基于 `log.Default()`），无法替换为业务方的日志实现 |
| fmt.Println 混用 | 部分调试信息通过 `fmt.Println` 输出，生产环境无法关闭，格式不统一 |
| 无日志级别 | 所有日志一视同仁，无法按级别过滤，生产环境噪声大 |
| 不支持结构化日志 | 日志输出为纯文本，无法携带结构化字段（如 SQL、耗时、影响行数），不利于日志采集和分析 |
| 不支持文件输出 | 日志只能输出到标准输出，无法满足生产环境文件输出、日志轮转等需求 |

### 1.2 改造目标

| 目标 | 说明 |
|------|------|
| 接口化 | 定义框架无关的 `Logger` 接口（`logx.Logger`），db 模块依赖抽象而非具体实现 |
| 环境适配 | 开发环境控制台输出，生产环境 JSON 文件输出 + 日志轮转，通过配置切换 |
| 公共抽取 | 日志能力抽取为独立的 `logx` 公共包，可被 `db`、`mq` 等多个模块复用 |
| GORM 适配 | 实现 `gorm.logger.Interface`，将 GORM 内部日志（Info/Warn/Error/Trace）委托给 `logx.Logger` |
| 向后兼容 | 保留 `InitMysqlClient` 等原有 API 签名不变，零配置即可运行 |

---

## 2. 整体架构设计

### 2.1 分层架构图

```
┌─────────────────────────────────────────────────────────┐
│                     业务应用层                            │
│  InitMysqlClient / InitMysqlClientWithOptions            │
│  db.WithLogger() / db.WithSQLLogger()                    │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                     db 模块                              │
│                                                         │
│  ┌──────────┐    ┌──────────────┐    ┌───────────────┐  │
│  │  option   │───▶│ dbConnect()  │───▶│  gormLogger   │  │
│  │(Option模式)│    │ (连接创建)    │    │(GORM 适配器)  │  │
│  └──────────┘    └──────────────┘    └───────┬───────┘  │
│                                              │          │
└──────────────────────────────────────────────┼──────────┘
                                               │ 委托
                                               ▼
┌─────────────────────────────────────────────────────────┐
│                    logx 公共日志包                        │
│                                                         │
│  ┌──────────┐    ┌──────────────┐    ┌───────────────┐  │
│  │  Logger   │    │  LogConfig   │    │  defaultLogger │  │
│  │  (接口)   │    │  (配置工厂)   │    │  (零配置兜底)  │  │
│  └─────┬────┘    └──────────────┘    └───────────────┘  │
│        │                                                │
│        │  SetLogger / GetLogger（全局注入）                │
│        │                                                │
└────────┼────────────────────────────────────────────────┘
         │ 实现
         ▼
┌─────────────────────────────────────────────────────────┐
│                   底层日志实现                            │
│                                                         │
│  ┌──────────────────┐    ┌───────────────────────────┐  │
│  │  zapx.ZapLogger   │    │  zapLoggerWrapper (内部)  │  │
│  │  (*zap.Logger适配) │    │  (InitLogger 内部包装)    │  │
│  └──────────────────┘    └───────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │  lumberjack (日志轮转，Rotation=true 时使用)       │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### 2.2 核心组件说明

| 组件 | 所在包 | 职责 |
|------|--------|------|
| `logx.Logger` | `logx` | 框架无关的日志接口，定义 Info/Warn/Error/Debug/WithFields 五个方法 |
| `logx.LogConfig` | `logx` | 日志配置结构，配合 `InitLogger` 工厂函数快速创建 Logger |
| `logx.GetLogger()` | `logx` | 全局 Logger 获取，三级优先级兜底 |
| `zapx.ZapLogger` | `logx/zapx` | 将业务方已有的 `*zap.Logger` 适配为 `logx.Logger` |
| `gormLogger` | `db` | 实现 `gorm.logger.Interface`，将 GORM 日志委托给 `logx.Logger` |
| `option` + `Option` | `db` | 函数式选项模式，支持灵活注入 Logger、SQL Logger、慢查询阈值等 |

---

## 3. 核心设计

### 3.1 logx 公共日志包

#### 接口定义

```go
// Logger 公共日志接口（框架无关）
type Logger interface {
    Info(msg string, fields ...LogField)
    Warn(msg string, fields ...LogField)
    Error(msg string, fields ...LogField)
    Debug(msg string, fields ...LogField)
    WithFields(fields ...LogField) Logger
}
```

接口使用自定义的 `LogField` 结构体，不绑定任何第三方日志库：

```go
type LogField struct {
    Key   string
    Value interface{}
}
```

#### 三级优先级

db 模块的日志解析遵循三级优先级链：

```
Option 注入（WithLogger / WithSQLLogger）> 全局 SetLogger > 默认控制台（defaultLogger）
```

- **最高优先级**：通过 `db.WithLogger(l)` 或 `db.WithSQLLogger(l)` 显式注入
- **中间优先级**：通过 `logx.SetLogger(l)` 全局注入，所有未显式注入的组件共用
- **兜底**：内置 `defaultLogger`（基于 `zap.NewDevelopmentConfig()`），输出到 stdout

#### 配置驱动工厂

通过 `LogConfig` + `InitLogger` 工厂函数，根据配置创建对应的 Logger：

| Mode | Rotation | 行为 |
|------|----------|------|
| `"console"`（默认） | - | zap development 格式输出到 stdout |
| `"file"` | `false` | zap production JSON 格式输出到指定文件 |
| `"file"` | `true` | lumberjack 日志轮转 + zap production JSON encoder |

#### 三种输出模式

1. **console 模式**：人类可读格式，适合开发调试
2. **file 模式**：JSON 格式写入指定文件，适合生产环境日志采集
3. **file + rotation 模式**：JSON 格式 + 自动轮转（按大小切割），支持压缩、保留天数、保留数量限制

### 3.2 GORM 适配器

#### 接口实现

`gormLogger` 实现了 `gorm.logger.Interface` 的全部方法：

```go
// gormLogger 实现 gorm.logger.Interface
type gormLogger struct {
    logger                    logx.Logger   // 业务日志
    sqlLogger                 logx.Logger   // SQL 日志（可分离）
    level                     gormlogger.LogLevel
    slowThreshold             time.Duration
    ignoreRecordNotFoundError bool
    enableSqlLog              bool          // SQL 日志开关，默认关闭
}
```

| 方法 | 说明 |
|------|------|
| `LogMode(level)` | 设置日志级别，返回新实例（不可变模式） |
| `Info(ctx, msg, args...)` | 记录 Info 级别日志，委托给 `logger.Info` |
| `Warn(ctx, msg, args...)` | 记录 Warn 级别日志，委托给 `logger.Warn` |
| `Error(ctx, msg, args...)` | 记录 Error 级别日志，委托给 `logger.Error` |
| `Trace(ctx, begin, fc, err)` | 记录 SQL 执行轨迹，核心方法 |

#### Trace 方法的 SQL 日志逻辑

`Trace` 是 GORM 适配器的核心方法，在每次 SQL 执行后被调用。处理流程：

```
Trace 被调用
    │
    ├─ level <= Silent → 直接返回（静默模式）
    │
    ├─ 计算耗时 elapsed = time.Since(begin)
    ├─ 获取 SQL 和影响行数: sql, rows = fc()
    │
    ├─ err != nil → Error 级别日志（logger）
    │     字段: sql, rows, elapsed, error
    │
    ├─ elapsed > slowThreshold → Warn 慢查询告警（sqlLogger）
    │     字段: sql, rows, elapsed
    │
    └─ enableSqlLog == true → Debug 普通 SQL 日志（sqlLogger）
          字段: sql, rows, elapsed
```

关键设计决策：
- **错误日志**始终输出（走 `logger` 业务日志通道）
- **慢查询告警**始终输出（走 `sqlLogger` SQL 日志通道）
- **普通 SQL 日志**默认关闭，需通过 `WithEnableSqlLog(true)` 显式开启，避免生产环境日志爆炸

#### toLogFields 转换

GORM 的 `Info/Warn/Error` 方法接收 `args ...interface{}`（key-value 交替排列），`toLogFields` 将其转换为 `[]logx.LogField`：

```go
func toLogFields(args []interface{}) []logx.LogField {
    // 每两个参数组成一个 LogField（key, value）
    // 奇数个参数时，最后一个作为 "extra" 字段
}
```

### 3.3 Option 模式

db 模块使用函数式选项模式（Functional Options），所有配置通过 `Option` 函数注入：

```go
type option struct {
    MaxOpenConn       int
    MaxIdleConn       int
    ConnMaxLifeSecond int
    PrepareStmt       bool
    LogName           string
    SlowLogMillSecond int64
    EnableSqlLog      bool
    logger            logx.Logger  // 业务日志
    sqlLogger         logx.Logger  // SQL 日志
}

type Option func(*option)
```

| Option 函数 | 作用 | 默认值 |
|-------------|------|--------|
| `WithMaxOpenConn(n)` | 最大打开连接数 | 1000 |
| `WithMaxIdleConn(n)` | 最大空闲连接数 | 100 |
| `WithConnMaxLifeSecond(n)` | 连接最大存活时间（秒） | 1800 |
| `WithPrepareStmt(b)` | 是否预编译语句 | true |
| `WithLogName(name)` | 日志名称 | `"gorm"` |
| `WithSlowLogMillSecond(ms)` | 慢查询阈值（毫秒） | 200 |
| `WithEnableSqlLog(b)` | 是否启用 SQL 日志打印 | false |
| `WithLogger(l)` | 注入业务日志 Logger | `logx.GetLogger()` |
| `WithSQLLogger(l)` | 注入 SQL 日志 Logger | 与业务日志相同 |

### 3.4 业务日志与 SQL 日志分离设计

db 模块支持将业务日志和 SQL 日志输出到不同的目标：

```
┌──────────────┐          ┌──────────────────┐
│  gormLogger   │─────────▶│  logger (业务日志) │──▶ Info/Warn/Error
│              │          │  (业务 Logger)    │    连接失败、迁移错误等
└──────┬───────┘          └──────────────────┘
       │
       │ sqlLogger        ┌──────────────────┐
       └─────────────────▶│  sqlLogger (SQL)  │──▶ 慢查询告警
                          │  (SQL Logger)    │    普通 SQL 日志
                          └──────────────────┘
```

分离逻辑在 `dbConnect` 中处理：

```go
// 日志兜底：未注入时使用 logx.GetLogger()
logger := opt.logger
if logger == nil {
    logger = logx.GetLogger()
}
sqlLogger := opt.sqlLogger
if sqlLogger == nil {
    sqlLogger = logger  // 未单独注入时，SQL 日志复用业务日志
}
```

典型使用场景：
- **开发阶段**：业务日志输出控制台，SQL 日志输出到文件，互不干扰
- **生产环境**：业务日志走控制台/文件，SQL 日志单独采集到 ELK

---

## 4. 使用方式

### 场景1: 零配置，默认控制台输出

不传入任何 Option，db 模块内部自动使用 `logx.GetLogger()`，日志输出到控制台。

```go
package main

import (
    "github.com/HeRedBo/pkg/db"
    "github.com/HeRedBo/pkg/logx"
)

func main() {
    // 零配置初始化，日志自动输出到控制台
    err := db.InitMysqlClient(db.DefaultClient, "root", "admin123", "localhost:3306", "demo")
    if err != nil {
        logx.GetLogger().Error("连接失败", logx.ErrField(err))
        return
    }

    client := db.GetMysqlClient(db.DefaultClient)
    // 使用 client.DB 进行 GORM 操作...
}
```

### 场景2: 注入自定义 zap Logger

业务方已有 zap Logger，通过 `zapx.New()` 适配为 `logx.Logger`，再通过 `WithLogger()` 注入。

```go
package main

import (
    "go.uber.org/zap"

    "github.com/HeRedBo/pkg/db"
    "github.com/HeRedBo/pkg/logx"
    "github.com/HeRedBo/pkg/logx/zapx"
)

func main() {
    // 业务方已有的 zap Logger
    zapLogger, _ := zap.NewProduction()
    defer zapLogger.Sync()

    // 通过 zapx 适配器包装为 logx.Logger，注入到 db 模块
    err := db.InitMysqlClientWithOptions(
        "zap-client",
        "root", "admin123", "localhost:3306", "demo",
        db.WithLogger(zapx.New(zapLogger)),
    )
    if err != nil {
        logx.GetLogger().Error("连接失败", logx.ErrField(err))
        return
    }

    client := db.GetMysqlClient("zap-client")
    // 使用 client.DB 进行 GORM 操作...
}
```

### 场景3: SQL 日志输出到文件（开发阶段）

使用 `logx.InitLogger` 创建文件日志 Logger，通过 `WithSQLLogger()` 注入，SQL 日志单独输出到文件。

```go
package main

import (
    "github.com/HeRedBo/pkg/db"
    "github.com/HeRedBo/pkg/logx"
)

func main() {
    // SQL 日志单独输出到文件，方便开发阶段排查
    sqlFileLogger := logx.InitLogger(logx.LogConfig{
        Mode:  "file",
        Path:  "./sql.log",
        Level: "debug",
    })

    err := db.InitMysqlClientWithOptions(
        "sqlfile-client",
        "root", "admin123", "localhost:3306", "demo",
        db.WithSQLLogger(sqlFileLogger),
        db.WithEnableSqlLog(true), // 开启普通 SQL 日志
    )
    if err != nil {
        logx.GetLogger().Error("连接失败", logx.ErrField(err))
        return
    }

    client := db.GetMysqlClient("sqlfile-client")
    // SQL 日志（含普通 SQL 和慢查询）将输出到 ./sql.log
    // 业务日志仍走控制台
}
```

### 场景4: 生产环境 ELK 集成（JSON 格式 + 日志轮转）

生产环境使用 JSON 格式 + lumberjack 日志轮转，方便 Filebeat/Fluentd 采集。

```go
package main

import (
    "github.com/HeRedBo/pkg/db"
    "github.com/HeRedBo/pkg/logx"
)

func main() {
    // 业务日志：JSON 格式 + 日志轮转
    bizLogger := logx.InitLogger(logx.LogConfig{
        Mode:       "file",
        Path:       "/var/log/app/db-biz.log",
        Level:      "info",
        Rotation:   true,
        MaxSize:    100,  // 单个文件最大 100MB
        MaxAge:     30,   // 保留 30 天
        MaxBackups: 5,    // 最多保留 5 个文件
        Compress:   true, // 压缩旧日志
    })

    // SQL 日志：独立文件，同样 JSON + 轮转
    sqlLogger := logx.InitLogger(logx.LogConfig{
        Mode:       "file",
        Path:       "/var/log/app/db-sql.log",
        Level:      "info",
        Rotation:   true,
        MaxSize:    200,
        MaxAge:     7,    // SQL 日志保留 7 天
        MaxBackups: 3,
        Compress:   true,
    })

    err := db.InitMysqlClientWithOptions(
        "prod-client",
        "root", "admin123", "localhost:3306", "demo",
        db.WithLogger(bizLogger),
        db.WithSQLLogger(sqlLogger),
        db.WithSlowLogMillSecond(500), // 慢查询阈值 500ms
    )
    if err != nil {
        bizLogger.Error("连接失败", logx.ErrField(err))
        return
    }

    // Filebeat 采集 /var/log/app/db-biz.log 和 db-sql.log 发送到 ELK
}
```

### 场景5: 动态开关 SQL 日志（WithEnableSqlLog）

通过 `WithEnableSqlLog` 控制普通 SQL 日志的开关。注意：错误日志和慢查询告警始终输出，不受此开关影响。

```go
package main

import (
    "os"

    "github.com/HeRedBo/pkg/db"
    "github.com/HeRedBo/pkg/logx"
)

func main() {
    // 根据环境变量动态控制 SQL 日志开关
    enableSQL := os.Getenv("ENABLE_SQL_LOG") == "true"

    err := db.InitMysqlClientWithOptions(
        "dynamic-client",
        "root", "admin123", "localhost:3306", "demo",
        db.WithEnableSqlLog(enableSQL),
        db.WithSlowLogMillSecond(200),
    )
    if err != nil {
        logx.GetLogger().Error("连接失败", logx.ErrField(err))
        return
    }

    // ENABLE_SQL_LOG=true 时：普通 SQL、慢查询、错误日志均输出
    // ENABLE_SQL_LOG=false 时：仅慢查询告警和错误日志输出
}
```

---

## 5. logx 公共包说明

### 5.1 Logger 接口定义

```go
// Logger 公共日志接口
type Logger interface {
    Info(msg string, fields ...LogField)
    Warn(msg string, fields ...LogField)
    Error(msg string, fields ...LogField)
    Debug(msg string, fields ...LogField)
    WithFields(fields ...LogField) Logger
}
```

- 所有方法使用 `LogField` 而非第三方库类型，保证框架无关性
- `WithFields` 返回新 Logger，预绑定字段后续自动携带

### 5.2 LogField 和辅助函数

```go
// LogField 日志字段（框架无关）
type LogField struct {
    Key   string
    Value interface{}
}

// Field 创建日志字段
func Field(key string, val interface{}) LogField

// ErrField 创建错误日志字段（key 固定为 "error"）
func ErrField(err error) LogField
```

使用示例：

```go
logger.Info("查询用户", logx.Field("user_id", 123), logx.Field("name", "redbo"))
logger.Error("连接失败", logx.ErrField(err))
```

### 5.3 LogConfig 配置项说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `Mode` | string | `"console"` | 日志模式：`"console"` 或 `"file"` |
| `Path` | string | - | 日志文件路径（`Mode=file` 时使用） |
| `Level` | string | `"info"` | 日志级别：`"debug"`, `"info"`, `"warn"`, `"error"` |
| `Compress` | bool | `false` | 是否压缩旧日志文件 |
| `KeepDays` | int | 0 | 日志保留天数（覆盖 `MaxAge`） |
| `MaxSize` | int | 100 | 单个日志文件最大 MB |
| `MaxAge` | int | 30 | 日志保留最大天数 |
| `MaxBackups` | int | 5 | 保留日志文件最大数量 |
| `Rotation` | bool | `false` | 是否启用日志轮转（`Mode=file` 时生效） |

### 5.4 InitLogger 三种模式说明

```go
func InitLogger(cfg LogConfig) Logger
```

| 模式 | 配置 | 输出格式 | 适用场景 |
|------|------|----------|----------|
| console | `Mode: "console"` 或不填 | zap development 人类可读格式 | 开发调试 |
| file | `Mode: "file"` | zap production JSON 格式 | 生产环境文件日志 |
| file + rotation | `Mode: "file"`, `Rotation: true` | JSON + lumberjack 自动轮转 | 生产环境长期运行 |

使用示例：

```go
// console 模式
consoleLogger := logx.InitLogger(logx.LogConfig{
    Level: "debug",
})

// file 模式
fileLogger := logx.InitLogger(logx.LogConfig{
    Mode:  "file",
    Path:  "/var/log/app/app.log",
    Level: "info",
})

// file + rotation 模式
rotationLogger := logx.InitLogger(logx.LogConfig{
    Mode:       "file",
    Path:       "/var/log/app/app.log",
    Level:      "info",
    Rotation:   true,
    MaxSize:    100,
    MaxAge:     30,
    MaxBackups: 5,
    Compress:   true,
})
```

### 5.5 zapx 适配器用法

`zapx` 包将业务方已有的 `*zap.Logger` 适配为 `logx.Logger`：

```go
import "github.com/HeRedBo/pkg/logx/zapx"

// 将 *zap.Logger 适配为 logx.Logger
zapLogger, _ := zap.NewProduction()
logger := zapx.New(zapLogger)

// 可直接用于 logx.SetLogger 全局注入
logx.SetLogger(logger)

// 或注入到具体模块
db.InitMysqlClientWithOptions("name", user, pass, host, db,
    db.WithLogger(zapx.New(zapLogger)),
)
```

---

## 6. 文件清单

### 新建文件

| 文件 | 职责 |
|------|------|
| `logx/logger.go` | `Logger` 接口定义、`LogField` 结构体、`Field`/`ErrField` 辅助函数、全局 `SetLogger`/`GetLogger`/`ResetLogger` |
| `logx/config.go` | `LogConfig` 配置结构、`InitLogger` 工厂函数（console/file/rotation 三种模式）、内部 `zapLoggerWrapper` 适配器 |
| `logx/default.go` | `defaultLogger` 零配置默认实现，基于 `zap.NewDevelopmentConfig()`，作为三级优先级的兜底 |
| `logx/zapx/zap_adapter.go` | `ZapLogger` 适配器，将 `*zap.Logger` 包装为 `logx.Logger` 接口 |
| `db/gorm_adapter.go` | `gormLogger` 适配器，实现 `gorm.logger.Interface`，将 GORM 日志委托给 `logx.Logger` |
| `db/test/main.go` | 使用示例，覆盖零配置、自定义 zap Logger、SQL 日志文件输出等场景 |

### 改造文件

| 文件 | 改造内容 |
|------|----------|
| `db/mysql.go` | 移除 `stdLogger` 硬编码和 `fmt.Println`；新增 `option` 结构体及 `WithLogger`/`WithSQLLogger`/`WithEnableSqlLog` 等 Option 函数；`dbConnect` 中使用 `logx.Logger` 创建 `gormLogger` 适配器 |
| `db/go.mod` | 新增 `logx` 模块依赖（`replace` 指向本地 `../logx`） |

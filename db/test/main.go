package main

import (
	"fmt"
	"time"

	"github.com/HeRedBo/pkg/db"
	"github.com/HeRedBo/pkg/logx"
	"github.com/HeRedBo/pkg/logx/zapx"
	"github.com/gookit/goutil/dump"
	"go.uber.org/zap"
)

// ─────────────────────────────────────────────
// User 测试用模型
// ─────────────────────────────────────────────

type User struct {
	ID            uint      `gorm:"primarykey;comment:主键"`
	Name          string    `gorm:"type:varchar(255);NOT NULL;DEFAULT:'';comment:用户名称"`
	Email         string    `gorm:"type:varchar(255);NOT NULL;DEFAULT:'';comment:用户邮箱"`
	Password      string    `gorm:"type:varchar(255);NOT NULL;DEFAULT:'';comment:密码"`
	RememberToken string    `gorm:"type:varchar(100);NOT NULL;DEFAULT:'';comment:认证token"`
	CreatedAt     time.Time `gorm:"autoCreateTime;comment:创建时间"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime;comment:更新时间"`
}

func (User) TableName() string {
	return "users"
}

// ─────────────────────────────────────────────
// 场景1: 零配置，默认控制台输出
// 不传入任何 Option，db 模块内部自动使用 logx.GetLogger()
// 日志输出到控制台，级别为 Info
// ─────────────────────────────────────────────

func scenario1_DefaultConsole() {
	fmt.Println("========== 场景1: 零配置，默认控制台输出 ==========")

	err := db.InitMysqlClient(db.DefaultClient, "root", "admin123", "localhost:3306", "demo")
	if err != nil {
		logx.GetLogger().Error("场景1 连接失败", logx.Field("client", db.DefaultClient), logx.ErrField(err))
		return
	}
	logx.GetLogger().Info("场景1 连接成功", logx.Field("client", db.DefaultClient))

	runDBOperations(db.DefaultClient)
}

// ─────────────────────────────────────────────
// 场景2: 注入自定义 zap Logger
// 业务方已有 zap Logger，通过 zapx.New() 适配为 logx.Logger
// 通过 db.WithLogger() 注入，业务日志和 SQL 日志共用该 Logger
// ─────────────────────────────────────────────

func scenario2_CustomZapLogger() {
	fmt.Println("========== 场景2: 注入自定义 zap Logger ==========")

	// 注入业务方已有的 zap Logger
	zapLogger, _ := zap.NewProduction()
	defer zapLogger.Sync()

	// 通过 zapx 适配器包装为 logx.Logger，注入到 db 模块
	err := db.InitMysqlClientWithOptions(
		"zap-client",
		"root", "admin123", "localhost:3306", "demo",
		db.WithLogger(zapx.New(zapLogger)),
	)
	if err != nil {
		logx.GetLogger().Error("场景2 连接失败", logx.ErrField(err))
		return
	}
	logx.GetLogger().Info("场景2 连接成功（使用自定义 zap Logger）")

	runDBOperations("zap-client")
}

// ─────────────────────────────────────────────
// 场景3: SQL 日志输出到文件（开发阶段）
// 使用 logx.InitLogger 创建文件日志 Logger
// 通过 db.WithSQLLogger() 注入，SQL 日志单独输出到文件
// 业务日志仍走控制台，方便开发阶段排查 SQL 问题
// ─────────────────────────────────────────────

func scenario3_SQLLogToFile() {
	fmt.Println("========== 场景3: SQL 日志输出到文件（开发阶段） ==========")

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
	)
	if err != nil {
		logx.GetLogger().Error("场景3 连接失败", logx.ErrField(err))
		return
	}
	logx.GetLogger().Info("场景3 连接成功（SQL 日志输出到 sql.log）")

	runDBOperations("sqlfile-client")
}

// ─────────────────────────────────────────────
// runDBOperations 公共数据库操作（三种场景复用）
// ─────────────────────────────────────────────

func runDBOperations(clientName string) {
	client := db.GetMysqlClient(clientName)
	if client == nil {
		logx.GetLogger().Error("获取客户端失败", logx.Field("client", clientName))
		return
	}
	ormDB := client.DB

	// 查看连接配置
	sqlDB, _ := ormDB.DB()
	dump.P("Stats:", sqlDB.Stats())

	// 建表
	if err := ormDB.AutoMigrate(&User{}); err != nil {
		logx.GetLogger().Error("AutoMigrate error", logx.ErrField(err))
		return
	}

	// 写入数据
	user := User{
		Name:          "redbo",
		Email:         "hhb@163.com",
		Password:      "",
		RememberToken: "",
	}
	if err := ormDB.Create(&user).Error; err != nil {
		logx.GetLogger().Error("insert error", logx.Field("user", user))
		return
	}

	// 查询数据
	users := make([]User, 0)
	ormDB.Where(&user).Find(&users)
	dump.Println(users)
}

func main() {
	// 场景1: 零配置，默认控制台输出
	scenario1_DefaultConsole()

	// 场景2: 注入自定义 zap Logger
	scenario2_CustomZapLogger()

	// 场景3: SQL 日志输出到文件（开发阶段）
	scenario3_SQLLogToFile()
}

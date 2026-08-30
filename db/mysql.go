package db

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"

	"github.com/HeRedBo/pkg/logx"
)

// DB 封装 gorm.DB，附带连接元信息
type DB struct {
	*gorm.DB
	ClientName string
	UserName   string
	password   string
	Host       string
	DBName     string
}

// option 连接配置项
type option struct {
	MaxOpenConn       int
	MaxIdleConn       int
	ConnMaxLifeSecond int
	PrepareStmt       bool
	LogName           string
	SlowLogMillSecond int64
	EnableSqlLog      bool
	logger            logx.Logger // 业务日志
	sqlLogger         logx.Logger // SQL 日志（可与业务日志分离）
}

// Option 函数式选项
type Option func(*option)

const (
	DefaultMaxOpenConn        = 1000
	DefaultMaxIdleConn        = 100
	DefaultConnMaxLifeSecond  = 1800 // 秒
	DefaultLogName            = "gorm"
	DefaultSlowLogMillisecond = 200
	DefaultClient             = "default-mysql-client"
	ReadClient                = "read-mysql"
	WriteClient               = "write-msql"
	TxClient                  = "tx-mysql"
)

var (
	MysqlClients = make(map[string]*DB)
	clientsMu    sync.RWMutex
)

// WithMaxOpenConn 设置最大打开连接数
func WithMaxOpenConn(maxOpenConn int) Option {
	return func(opt *option) {
		opt.MaxOpenConn = maxOpenConn
	}
}

// WithMaxIdleConn 设置最大空闲连接数
func WithMaxIdleConn(maxIdleConn int) Option {
	return func(opt *option) {
		opt.MaxIdleConn = maxIdleConn
	}
}

// WithConnMaxLifeSecond 设置连接最大存活时间（秒）
func WithConnMaxLifeSecond(connMaxLifeSecond int) Option {
	return func(opt *option) {
		opt.ConnMaxLifeSecond = connMaxLifeSecond
	}
}

// WithLogName 设置日志名称
func WithLogName(logName string) Option {
	return func(opt *option) {
		opt.LogName = logName
	}
}

// WithSlowLogMillSecond 设置慢查询阈值（毫秒）
func WithSlowLogMillSecond(slowLogMillSecond int64) Option {
	return func(opt *option) {
		opt.SlowLogMillSecond = slowLogMillSecond
	}
}

// WithPrepareStmt 设置是否预编译语句
func WithPrepareStmt(prepareStmt bool) Option {
	return func(opt *option) {
		opt.PrepareStmt = prepareStmt
	}
}

// WithEnableSqlLog 设置是否启用 SQL 日志
func WithEnableSqlLog(enableSqlLog bool) Option {
	return func(opt *option) {
		opt.EnableSqlLog = enableSqlLog
	}
}

// WithLogger 注入业务日志 Logger
func WithLogger(l logx.Logger) Option {
	return func(opt *option) {
		opt.logger = l
	}
}

// WithSQLLogger 注入 SQL 日志 Logger（可与业务日志分离）
func WithSQLLogger(l logx.Logger) Option {
	return func(opt *option) {
		opt.sqlLogger = l
	}
}

// InitMysqlClient 使用默认配置初始化 MySQL 客户端
func InitMysqlClient(clientName, username, password, host, dbName string) error {
	if len(clientName) == 0 {
		return errors.New("client name is empty")
	}
	if len(username) == 0 {
		return errors.New("username is empty")
	}
	opt := &option{
		MaxOpenConn:       DefaultMaxOpenConn,
		MaxIdleConn:       DefaultMaxIdleConn,
		ConnMaxLifeSecond: DefaultConnMaxLifeSecond,
		PrepareStmt:       true,
	}

	db, err := dbConnect(username, password, host, dbName, opt)
	if err != nil {
		return err
	}
	clientsMu.Lock()
	MysqlClients[clientName] = &DB{
		DB:         db,
		ClientName: clientName,
		UserName:   username,
		password:   password,
		Host:       host,
		DBName:     dbName,
	}
	clientsMu.Unlock()
	return nil
}

// InitMysqlClientWithOptions 使用自定义选项初始化 MySQL 客户端
func InitMysqlClientWithOptions(clientName, username, password, host, dbName string, options ...Option) error {
	if len(clientName) == 0 {
		return errors.New("client name is empty")
	}
	if len(username) == 0 {
		return errors.New("username is empty")
	}
	opt := &option{}
	for _, f := range options {
		if f != nil {
			f(opt)
		}
	}
	db, err := dbConnect(username, password, host, dbName, opt)
	if err != nil {
		return err
	}
	clientsMu.Lock()
	MysqlClients[clientName] = &DB{
		DB:         db,
		ClientName: clientName,
		UserName:   username,
		password:   password,
		Host:       host,
		DBName:     dbName,
	}
	clientsMu.Unlock()
	return nil
}

// GetMysqlClient 根据名称获取 MySQL 客户端
func GetMysqlClient(clientName string) *DB {
	clientsMu.RLock()
	defer clientsMu.RUnlock()
	if client, ok := MysqlClients[clientName]; ok {
		return client
	}
	return nil
}

// CloseMysqlClient 关闭指定名称的 MySQL 客户端
func CloseMysqlClient(clientName string) error {
	client := GetMysqlClient(clientName)
	if client == nil {
		return fmt.Errorf("mysql client %q not found", clientName)
	}
	sqlDB, err := client.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// dbConnect 创建数据库连接
func dbConnect(user, pass, host, dbName string, opt *option) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=%t&loc=%s",
		user,
		pass,
		host,
		dbName,
		true,
		"Local")

	// 日志兜底：未注入时使用 logx.GetLogger()
	logger := opt.logger
	if logger == nil {
		logger = logx.GetLogger()
	}
	sqlLogger := opt.sqlLogger
	if sqlLogger == nil {
		sqlLogger = logger
	}

	// 慢查询阈值
	slowThreshold := time.Duration(opt.SlowLogMillSecond) * time.Millisecond
	if slowThreshold == 0 {
		slowThreshold = time.Duration(DefaultSlowLogMillisecond) * time.Millisecond
	}

	// 创建 gormLogger 适配器作为 GORM 的 Logger
	gormLog := NewGormLogger(logger, sqlLogger, slowThreshold, opt.EnableSqlLog)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		//为了确保数据一致性，GORM 会在事务里执行写入操作（创建、更新、删除）
		//如果没有这方面的要求，可以设置SkipDefaultTransaction为true来禁用它。
		//SkipDefaultTransaction: true,
		Logger:      gormLog,
		PrepareStmt: opt.PrepareStmt,
		NamingStrategy: schema.NamingStrategy{
			//使用单数表名,默认为复数表名，即当model的结构体为User时，默认操作的表名为users
			//设置	SingularTable: true 后当model的结构体为User时，操作的表名为user
			SingularTable: true,
			//TablePrefix: "pre_", //表前缀
		},
	})
	if err != nil {
		// 后续优化 error 处理
		return nil, err
	}

	db.Set("gorm:table_options", "CHARSET=utf8mb4")
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// 设置连接池 用于设置最大打开的连接数，默认值为0表示不限制.设置最大的连接数，可以避免并发太高导致连接mysql出现too many connections的错误。
	if opt.MaxOpenConn > 0 {
		sqlDB.SetMaxOpenConns(opt.MaxOpenConn)
	} else {
		sqlDB.SetMaxIdleConns(DefaultMaxOpenConn)
	}

	// 设置最大连接数 用于设置闲置的连接数.设置闲置的连接数则当开启的一个连接使用完成后可以放在池里等候下一次使用。
	if opt.MaxIdleConn > 0 {
		sqlDB.SetMaxIdleConns(opt.MaxIdleConn)
	}

	// 设置最大连接超时时间
	if opt.ConnMaxLifeSecond > 0 {
		sqlDB.SetConnMaxLifetime(time.Second * time.Duration(opt.ConnMaxLifeSecond))
	}

	return db, nil
}

package db

import (
	"errors"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"log"
	"os"
	"time"
)

type DB struct {
	*gorm.DB
	ClientName string
	UserName   string
	password   string
	Host       string
	DBName     string
}

type option struct {
	MaxOpenConn       int
	MaxIdleConn       int
	ConnMaxLifeSecond time.Duration
	PrepareStmt       bool
	LogName           string
	SlowLogMillSecond int64
	EnableSqlLog      bool
}

type Option func(*option)

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

const (
	DefaultMaxOpenConn         = 1000
	DefaultMaxIdleConn         = 100
	DefaultConnMaxLifeSecond   = 30 * time.Minute
	DefaultLogName             = "gorm"
	DefaultSlowLogMilllisecond = 200
	DefaultClient              = "default-mysql-client"
	ReadClient                 = "read-mysql"
	WirteClient                = "write-msql"
	TxClient                   = "tx-mysql"
)

var (
	MysqlClitents = make(map[string]*DB)
	MysqltdLogger stdLogger
)

func init() {
	MysqltdLogger = log.New(os.Stdout, "[gorm]", log.LstdFlags|log.Lshortfile)
}

func (o *option) reset() {
	o.MaxOpenConn = 0
	o.MaxIdleConn = 0
	o.ConnMaxLifeSecond = 0
	o.LogName = DefaultLogName
	o.PrepareStmt = false
	o.SlowLogMillSecond = DefaultSlowLogMilllisecond
}

func WithMaxOpenConn(maxOpenConn int) Option {
	return func(opt *option) {
		opt.MaxOpenConn = maxOpenConn
	}
}

func WithMaxIdleConn(maxIdleConn int) Option {
	return func(opt *option) {
		opt.MaxIdleConn = maxIdleConn
	}
}

func WithConnMaxLifeSecond(connMaxLifeTime time.Duration) Option {
	return func(opt *option) {
		opt.ConnMaxLifeSecond = connMaxLifeTime
	}
}

func WithLogName(logName string) Option {
	return func(opt *option) {
		opt.LogName = logName
	}
}

func WithSlowLogMillSecond(slowLogMillSecond int64) Option {
	return func(opt *option) {
		opt.SlowLogMillSecond = slowLogMillSecond
	}
}

func WithPrepareStmt(prepareStmt bool) Option {
	return func(opt *option) {
		opt.PrepareStmt = prepareStmt
	}
}

func WithEnableSqlLog(enableSqlLog bool) Option {
	return func(opt *option) {
		opt.EnableSqlLog = enableSqlLog
	}
}

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
	MysqlClitents[clientName] = &DB{
		DB:         db,
		ClientName: clientName,
		UserName:   username,
		password:   password,
		Host:       host,
		DBName:     dbName,
	}
	return nil
}

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
	MysqlClitents[clientName] = &DB{
		DB:         db,
		ClientName: clientName,
		UserName:   username,
		password:   password,
		Host:       host,
		DBName:     dbName,
	}
	return nil
}

func GetMysqlClient(clientName string) *DB {
	if client, ok := MysqlClitents[clientName]; ok {
		return client
	}
	return nil
}

func CloseMysqlClient(clitentName string) error {
	sqlDB, err := GetMysqlClient(clitentName).DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func dbConnect(user, pass, host, dbName string, option *option) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=%t&loc=%s",
		user,
		pass,
		host,
		dbName,
		true,
		"Local")

	if option.SlowLogMillSecond == 0 {
		option.SlowLogMillSecond = DefaultSlowLogMilllisecond
	}
	Log := logger.New(MysqltdLogger, logger.Config{
		SlowThreshold:             time.Duration(option.SlowLogMillSecond) * time.Millisecond,
		LogLevel:                  logger.Warn,
		IgnoreRecordNotFoundError: true,
		Colorful:                  true,
	})

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		//为了确保数据一致性，GORM 会在事务里执行写入操作（创建、更新、删除）
		//如果没有这方面的要求，可以设置SkipDefaultTransaction为true来禁用它。
		//SkipDefaultTransaction: true,
		Logger:      Log,
		PrepareStmt: option.PrepareStmt,
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
	if option.MaxOpenConn > 0 {
		sqlDB.SetMaxOpenConns(option.MaxOpenConn)
	} else {
		sqlDB.SetMaxIdleConns(DefaultMaxOpenConn)
	}

	// 设置最大连接数 用于设置闲置的连接数.设置闲置的连接数则当开启的一个连接使用完成后可以放在池里等候下一次使用。
	if option.MaxIdleConn > 0 {
		sqlDB.SetMaxIdleConns(option.MaxIdleConn)
	}

	// 设置最大连接超时时间
	if option.ConnMaxLifeSecond > 0 {
		sqlDB.SetConnMaxLifetime(time.Second * option.ConnMaxLifeSecond)
	}

	// 注册钩子函数 实现 实时 打印 执行SQL功能
	err = db.Callback().Create().After("gorm:after_create").Register(DefaultLogName, afterLog)
	if err != nil {
		MysqltdLogger.Print("Register Create error", err)
	}
	err = db.Callback().Query().After("gorm.after_query").Register(DefaultLogName, afterLog)
	if err != nil {
		MysqltdLogger.Print("Register Query error", err)
	}
	err = db.Callback().Update().After("gorm:after_update").Register(DefaultLogName, afterLog)
	if err != nil {
		MysqltdLogger.Print("Register Update error", err)
	}
	err = db.Callback().Delete().After("gorm:after_delete").Register(DefaultLogName, afterLog)
	if err != nil {
		MysqltdLogger.Print("Register Delete error", err)
	}
	return db, nil
}

func afterLog(db *gorm.DB) {
	err := db.Error
	sql := db.Dialector.Explain(db.Statement.SQL.String(), db.Statement.Vars...)
	if err != nil {
		MysqltdLogger.Print(sql, err)
	} else {
		fmt.Println("[ SQL语句 ]", sql)
	}
}

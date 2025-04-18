package main

import (
	"pkg/db"
	"time"

	"github.com/gookit/goutil/dump"
	"go.uber.org/zap"
)

func initMysql() {
	err := db.InitMysqlClient(db.DefaultClient, "root", "admin123", "localhost:3306", "demo")
	if err != nil {
		db.MysqltdLogger.Print("InitMysqlClient client error" + db.DefaultClient)
		return
	}
	db.MysqltdLogger.Print("connect mysql success ", zap.String("client", db.DefaultClient))
	dump.P("connect mysql success ", zap.String("client", db.DefaultClient))
	err = db.InitMysqlClientWithOptions(db.TxClient, "root", "admin123", "localhost:3306", "shop", db.WithPrepareStmt(false))
	if err != nil {
		db.MysqltdLogger.Print("InitMysqlClient client error" + db.TxClient)
		return
	}
}

//	type User struct {
//		gorm.Model
//	}
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

func main() {
	initMysql()

	ormDB := db.GetMysqlClient(db.DefaultClient).DB

	// 查看链接配置
	sqlDB, _ := ormDB.DB()
	db.MysqltdLogger.Printf("Stats : %+v", sqlDB.Stats())
	//建表
	if err := ormDB.AutoMigrate(&User{}); err != nil {
		db.MysqltdLogger.Print("AutoMigrate user error", err)
	}

	//自定义表名的另一种方式
	//if err := ormDB.Table("users2").AutoMigrate(&User{}); err != nil {
	//	db.MysqltdLogger.Print("AutoMigrate user error", zap.Error(err))
	//}
	//写入数据
	user := User{
		Name: "redbo",
		//Name:     &name,
		Email:         "hhb@163.com",
		Password:      "",
		RememberToken: "",
	}
	// if err := ormDB.Create(&user).Error; err != nil {
	// 	//db.MysqltdLogger.Print("insert error", zap.Any("user", user))
	// 	dump.Print("insert error", zap.Any("user", user))
	// }

	// 指定字段创建
	if err := ormDB.Select("email").Create(&user).Error; err != nil {
		db.MysqltdLogger.Print("insert error", zap.Any("user", user))
	}
	return

	//var users = []User{{Name: "user1", Email: "u1"}, {Name: "user2", Email: "u2"}, {Name: "user3", Email: "u3"}}
	//if err := ormDB.Create(&users).Error; err != nil {
	//	db.MysqltdLogger.Print("insert error", zap.Any("user", user))
	//}
	users := make([]User, 0)
	ormDB.Where(&user).Find(&users)
	db.MysqltdLogger.Printf("%+v", users)
	dump.Println(users)
}

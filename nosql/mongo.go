package nosql

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"os"
	"strings"
	"time"
)

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type MgClient struct {
	*mongo.Client
}

type CursorCallBackFunc func(res interface{}, err error)

var (
	mongoClinets   = map[string]*MgClient{}
	MongoStdLogger stdLogger
)

func init() {
	MongoStdLogger = log.New(os.Stdout, "[Mongo]", log.LstdFlags|log.Lshortfile)
	mongoClinets = make(map[string]*MgClient)
}

const (
	DefaultMongoClient    = "default-mongo"
	DefaultConnectTimeout = 3 * time.Second
)

func InitMongoClient(clientName, username, password string, addrs []string, mongoPoolLimit uint64) error {
	hosts := strings.Join(addrs, ",")
	auth := ""
	if len(username) > 0 && len(password) > 0 {
		auth = username + ":" + password + "@"
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultConnectTimeout)
	defer cancel()

	// example mongodb://username:password@192.168.1.99:27017,192.168.1.88:27017,192.168.1.66:27017
	mongoURL := fmt.Sprintf("mongodb://%s%s", auth, hosts)
	MongoStdLogger.Print("mongoURL : ", mongoURL)
	opt := options.Client().ApplyURI(mongoURL)
	opt.SetReadPreference(readpref.SecondaryPreferred()) //优先读从库
	//opt.SetMaxConnIdleTime(30 * time.Minute)   //指定连接可以保持空闲的最时间（默认无限制）
	opt.SetMaxPoolSize(mongoPoolLimit)     //使用最大的连接数
	opt.SetMinPoolSize(mongoPoolLimit / 4) //最小连接数，默认是0

	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		return err
	}
	//检测服务是否已连接
	if err := client.Ping(getContext(), readpref.Primary()); err != nil {
		return err
	}

	mongoClient := MgClient{client}
	mongoClinets[clientName] = &mongoClient
	return nil
}

func GeMongoClient(clientName string) *MgClient {
	if client, ok := mongoClinets[clientName]; ok {
		return client
	}
	MongoStdLogger.Print("Call 'InitMongo' before!")
	return nil
}

func getContext() (ctx context.Context) {
	ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
	return
}

// InsertMany example ("db","table",bson.D{{"name", "Alice"}},bson.D{{"name", "Bob"}})
func (client *MgClient) InsertMany(db, table string, docs ...interface{}) error {
	_, err := client.Database(db).Collection(table).InsertMany(getContext(), docs)
	return err
}

/**
InsertManyTryBest 上面的InsertMany在遇到异常的时候（比如插入mongo集群中已存在的数据），全部文档都会插入失败
方法则忽略异常的文档，将没出问题的这部分文档写入到mongo
检测插入过程的错误可以使用下面的方式
err := GetMongoClient(DefaultMongoClient).InsertManyTryBest("db", "table", doc)
we, ok := err.(mongo.BulkWriteException)
if ok {
	TO DO ...
}
出现重复文档的code = 11000
if we.HasErrorCode(11000) {
	TO DO ...
}
*/

func (client *MgClient) InsertManyTryBest(db, table string, docs ...interface{}) error {
	var err error
	collection := client.Database(db).Collection(table)
	ordered := false
	opts := []*options.InsertManyOptions{{
		Ordered: &ordered,
	}}
	if _, err = collection.InsertMany(getContext(), docs, opts...); err != nil {
		return err
	}
	return nil
}

// Upsert doc是bson格式
func (client *MgClient) Upsert(db, table string, filter bson.D, doc interface{}) error {
	collection := client.Database(db).Collection(table)
	//设置Upset模式
	opts := options.FindOneAndUpdate().SetUpsert(true)
	return collection.FindOneAndUpdate(getContext(), filter, bson.D{{"$set", doc}}, opts).Err()
}

// Close 关闭链接
func (client *MgClient) Close() {
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := client.Disconnect(ctx)
	if err != nil {
		MongoStdLogger.Print("mongo close error ", err)
	}
	MongoStdLogger.Print("closed : mongodb")
}

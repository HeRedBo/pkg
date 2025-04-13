package nosql

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
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

func (client *MgClient) ReplaceOne(db, table string, filter bson.D, doc interface{}) error {
	collection := client.Database(db).Collection(table)

	opts := options.Replace().SetUpsert(true)
	_, err := collection.ReplaceOne(getContext(), filter, doc, opts)
	return err
}

func (client *MgClient) UpdateOne(db, table string, filter bson.D, update interface{}) error {
	_, err := client.Database(db).Collection(table).UpdateOne(getContext(), filter, bson.M{"$set": update}, nil)
	return err
}

func (client *MgClient) UpdateMany(db, table string, filter bson.D, update interface{}) error {
	_, err := client.Database(db).Collection(table).UpdateMany(getContext(), filter, update, nil)
	return err
}

func (client *MgClient) Find(db, table string, filter bson.D, result interface{}) (bool, error) {
	// 选择数据库和合集
	var (
		cursor *mongo.Cursor
		err    error
	)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), filter); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return false, err
	}

	if err = cursor.Err(); err != nil {
		return false, err
	}

	defer cursor.Close(context.Background())
	err = cursor.All(context.Background(), result)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (client *MgClient) FindWithOrder(db, table string, filter bson.D, orders map[string]int, result interface{}) (bool, error) {
	var (
		cursor *mongo.Cursor
		err    error
	)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), filter); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return false, err
	}

	findOptions := options.Find()
	for filed, sort := range orders {
		findOptions.SetSort(bson.D{{filed, sort}})
	}

	if cursor, err = collection.Find(getContext(), filter); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return false, err
	}

	if err = cursor.Err(); err != nil {
		return false, err
	}

	defer cursor.Close(context.Background())
	err = cursor.All(context.Background(), result)
	if err != nil {
		return false, err
	}
	return true, nil
}

// FindOne 查询一条数据
// query example bson.D{{"name", 1}, {"age", 1}}
func (client *MgClient) FindOne(db, table string, filter bson.D, resultObj interface{}) error {
	result := client.Database(db).Collection(table).FindOne(getContext(), filter)
	if result.Err() != nil && !errors.Is(result.Err(), mongo.ErrNoDocuments) {
		return result.Err()
	}
	if result.Decode(resultObj) != mongo.ErrNoDocuments {
		return result.Decode(resultObj)
	}
	return nil
}

func (client *MgClient) FindByID(db, table string, id interface{}, resultObj interface{}) error {
	result := client.Database(db).Collection(table).FindOne(getContext(), bson.D{{"_id", id}})
	if result.Err() != nil && !errors.Is(result.Err(), mongo.ErrNoDocuments) {
		return result.Err()
	}
	return result.Decode(resultObj)
}

func (client *MgClient) FindWithOpts(db, table string, offset, limit int64, filter interface{}, opts *options.FindOptions, result interface{}) (bool, error) {
	var (
		cursor *mongo.Cursor
		err    error
	)
	opts.SetLimit(limit).SetSkip(offset)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), filter, opts); err != nil {
		return false, err
	}
	if err = cursor.Err(); err != nil {
		return false, err
	}

	defer cursor.Close(context.Background())
	err = cursor.All(context.Background(), result)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (client *MgClient) FindUseCursor(db, table string, batchSize int32, filter bson.D, rowType interface{}, cursorCallBackFunc CursorCallBackFunc) error {
	var (
		cursor *mongo.Cursor
		err    error
	)

	opts := &options.FindOptions{}
	opts.SetBatchSize(batchSize)
	collection := client.Database(db).Collection(table)

	if cursor, err = collection.Find(getContext(), filter, opts); err != nil {
		return err
	}
	if err = cursor.Err(); err != nil {
		return err
	}

	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		err = cursor.Decode(rowType)
		// 回调结果
		cursorCallBackFunc(rowType, err)
	}
	return err
}

func (client *MgClient) FindUseCursorWithOptions(db, table string, batchSize int32, filter bson.D, rowType interface{}, opts *options.FindOptions,
	cursorCallbackFunc CursorCallBackFunc) error {
	var (
		cursor *mongo.Cursor
		err    error
	)
	opts.SetBatchSize(batchSize)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), filter, opts); err != nil {
		return err
	}
	if err = cursor.Err(); err != nil {
		return err
	}

	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		err = cursor.Decode(rowType)
		// 回调结果
		cursorCallbackFunc(rowType, err)
	}
	return err
}

func (client *MgClient) AggregateUseCursor() {

}

func (client *MgClient) DeleteOne() {

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

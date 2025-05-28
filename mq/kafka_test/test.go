package main

import (
	"encoding/json"
	"fmt"
	"github.com/HeRedBo/pkg/mq"
	"github.com/IBM/sarama"
	"github.com/gookit/goutil/dump"
	"time"
)

var (
	//hosts = []string{"host.docker.internal:9092"}
	hosts = []string{"127.0.0.1:9092"}
	topic = "test-topic"
)

type Msg struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	CreateAt int64  `json:"create_at"`
}

func main() {
	//productSyncMsg()

	producrAsyncMsg()

	//consumeMsg()
}

func productSyncMsg() {
	err := mq.InitSyncKafkaProducer(mq.DefaultKafkaSyncProducer, hosts, nil)
	if err != nil {
		dump.Println("InitSyncKafkaProducer error", err)
		return
	}
	msg := Msg{
		ID:       1,
		Name:     "test name sync",
		CreateAt: time.Now().Unix(),
	}

	msgBody, _ := json.Marshal(msg)
	partion, offset, err := mq.GetKafkaSyncProducer(mq.DefaultKafkaSyncProducer).Send(&sarama.ProducerMessage{
		Topic:     topic,
		Value:     mq.KafkaMsgValueEncoder(msgBody),
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		fmt.Println("Send msg error", err)
	} else {
		fmt.Println("Send msg success partion ", partion, "offset", offset)
	}
}

func producrAsyncMsg() {
	err := mq.InitAsyncKafkaProducer(mq.DefaultKafkaAsyncProducer, hosts, nil)
	if err != nil {
		dump.Println("InitSyncKafkaProducer error", err)
		return
	}

	msg := Msg{
		ID:       1,
		Name:     "test name async",
		CreateAt: time.Now().Unix(),
	}
	msgBody, _ := json.Marshal(msg)

	err = mq.GetKafkaAsyncProducer(mq.DefaultKafkaAsyncProducer).Send(&sarama.ProducerMessage{Topic: topic, Value: mq.KafkaMsgValueEncoder(msgBody)})
	if err != nil {
		fmt.Println("Send msg error", err)
	} else {
		fmt.Println("Send msg success")
	}
	//异步提交需要等待
	time.Sleep(3 * time.Second)
}

//
//func consumeMsg() {
//	_, err := mq.StartKafkaConsumer(hosts, []string{topic}, "test-group", nil, msgHandler)
//	if err != nil {
//		fmt.Println(err)
//	}
//	signals := make(chan os.Signal, 1)
//	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
//	select {
//	case s := <-signals:
//		mq.KafkaStdLogger.Println("kafka test receive system signal:", s)
//		return
//	}
//}
//
//func msgHandler(message *sarama.ConsumerMessage) (bool, error) {
//
//	fmt.Println("消费消息:", "topic:", message.Topic, "Partition:", message.Partition, "Offset:", message.Offset, "value:", string(message.Value))
//	msg := Msg{}
//	err := json.Unmarshal(message.Value, &msg)
//	if err != nil {
//		//解析不了的消息怎么处理？
//		dump.Println("Unmarshal error", zap.Error(err))
//		//logger.Error("Unmarshal error", zap.Error(err))
//		return true, nil
//	}
//	fmt.Println("msg : ", msg)
//	return true, nil
//}

package main

import (
	"encoding/json"
	"fmt"
	"github.com/HeRedBo/pkg/mq"
	"github.com/IBM/sarama"
	"github.com/gookit/goutil/dump"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

var (
	hosts = []string{"127.0.0.1:9092"}
	topic = "test-topic"
)

type Msg struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	CreateAt int64  `json:"create_at"`
}

func main() {
	consumeMsg()
}

func consumeMsg() {
	_, err := mq.StartKafkaConsumer(hosts, []string{topic}, "test-group", nil, msgHandler)
	if err != nil {
		fmt.Println(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	select {
	case s := <-signals:
		mq.KafkaStdLogger.Println("kafka test receive system signal:", s)
		return
	}
}

func msgHandler(message *sarama.ConsumerMessage) (bool, error) {
	fmt.Println("消费消息:", "topic:", message.Topic, "Partition:", message.Partition, "Offset:", message.Offset, "value:", string(message.Value))
	msg := Msg{}
	err := json.Unmarshal(message.Value, &msg)
	if err != nil {
		//解析不了的消息怎么处理？
		dump.Println("Unmarshal error", zap.Error(err))
		//logger.Error("Unmarshal error", zap.Error(err))
		return true, nil
	}
	fmt.Println("msg : ", msg)
	return true, nil
}

package mq

import (
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type KafkaProducer struct {
	Name       string
	Hosts      []string
	Config     *sarama.Config
	Status     string
	Breaker    *breaker.Breaker
	ReConnect  chan bool
	StatusLock sync.Mutex
}

// Kafka 消息发送结构体
type Kafka struct {
	Topic     string
	KeyBytes  []byte
	DataBytes []byte
}

// SyncProducer 同步生产者
type SyncProducer struct {
	KafkaProducer
	SyncProducer *sarama.SyncProducer
}

// AsyncProducer 异步生产者
type AsyncProducer struct {
	KafkaProducer
	AsyncProducer *sarama.AsyncProducer
}

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// region 常量
const (
	// KafkaProducerConnected 生产者已连接
	KafkaProducerConnected string = "connected"
	// KafkaProducerDisconnected 生产者已断开
	KafkaProducerDisconnected string = "disconnected"
	// KafkaProducerClosed 生产者已关闭
	KafkaProducerClosed string = "closed"

	DefaultKafkaAsyncProducer = "default-kafka-async-producer"
	DefaultKafkaSyncProducer  = "default-kafka-sync-producer"
)

// endregion

// region 定义变量
var (
	ErrProducerTimeout  = errors.New("push message timeout")
	KafkaSyncProducers  = make(map[string]*SyncProducer)
	KafkaAsyncProducers = make(map[string]*AsyncProducer)
	KafkaStdLogger      stdLogger
)

// endregion

func init() {
	KafkaStdLogger = log.New(os.Stdout, "[kafka]", log.LstdFlags|log.Lshortfile)
}

func KafkaMsgValueEncoder(value []byte) sarama.Encoder {
	return sarama.ByteEncoder(value)
}

func KafkaMsgValueStrEncoder(value string) sarama.Encoder {
	return sarama.StringEncoder(value)
}

// kafka默认生产者配置
func getDefaultProducerConfig(clientID string) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V4_0_0_0                       // 设置 Kafka 版本
	config.Net.DialTimeout = time.Second * 30              // 初始连接超时时间
	config.Net.WriteTimeout = time.Second * 30             // 读取超时
	config.Net.ReadTimeout = time.Second * 30              // 写入超时
	config.Producer.Retry.Max = 5                          // 最大重试次数
	config.Producer.Retry.Backoff = 500 * time.Millisecond // 重试间隔
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	//需要小于broker的 `message.max.bytes`配置，默认是1000000
	config.Producer.MaxMessageBytes = 1000000 * 2
	//config.Producer.RequiredAcks = sarama.WaitForLocal // WaitForLocal 表示生产者只需要等待消息被写入到分区的首领副本
	config.Producer.RequiredAcks = sarama.WaitForAll // 等待所有副本确认 WaitForAll 表示生产者需要等待消息被写入到所有的副本（首领副本和所有跟随者副本）
	//config.Producer.Partitioner = sarama.NewRandomPartitioner
	//config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Partitioner = sarama.NewHashPartitioner
	// zstd 算法有着最高的压缩比，而在吞吐量上的表现只能说中规中矩，LZ4 > Snappy > zstd 和 GZIP
	//LZ4 具有最高的吞吐性能，压缩比zstd > LZ4 > GZIP > Snappy
	//综上LZ4性价比最高
	config.Producer.Compression = sarama.CompressionLZ4      // 压缩方式
	config.Producer.Flush.Frequency = 500 * time.Millisecond // 批量发送频率
	config.Producer.Flush.MaxMessages = 1000                 // 批量最大消息数
	return
}

// 初始化同步生产者
func InitSyncKafkaProducer(name string, hosts []string, config *sarama.Config) error {
	syncProducer := &SyncProducer{}
	syncProducer.Name = name
	syncProducer.Hosts = hosts
	syncProducer.Status = KafkaProducerDisconnected
	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	syncProducer.Config = config
	if producer, err := sarama.NewSyncProducer(hosts, config); err != nil {
		return errors.New(fmt.Sprintf("创建生产者失败: %v", err))
	} else {
		// 3次失败 → 熔断 → 2秒后半开 → 成功恢复
		syncProducer.Breaker = breaker.New(3, 1, 2*time.Second)
		syncProducer.ReConnect = make(chan bool)
		syncProducer.SyncProducer = &producer
		syncProducer.Status = KafkaProducerConnected
		//  断开重连
		go syncProducer.keepConnect()
		// 连接状态检查
		go syncProducer.check()
		//logger.Info("SyncKakfaProducer connected name " + name)
	}
	KafkaSyncProducers[name] = syncProducer
	return nil
}

// 初始化异步生产者
func InitAsyncKafkaProducer(name string, hosts []string, config *sarama.Config) error {
	asyncProducer := &AsyncProducer{}
	asyncProducer.Name = name
	asyncProducer.Hosts = hosts
	asyncProducer.Status = KafkaProducerDisconnected
	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	asyncProducer.Config = config
	if producer, err := sarama.NewAsyncProducer(hosts, config); err != nil {
		return errors.New(fmt.Sprintf("创建生产者失败: %v", err))
	} else {
		asyncProducer.Breaker = breaker.New(3, 1, 2*time.Second)
		asyncProducer.ReConnect = make(chan bool)
		asyncProducer.AsyncProducer = &producer
		asyncProducer.Status = KafkaProducerConnected
		//  断开重连
		go asyncProducer.keepConnect()
		//// 连接状态检查
		go asyncProducer.check()
		//logger.Info("SyncKakfaProducer connected name " + name)
	}
	KafkaAsyncProducers[name] = asyncProducer
	return nil
}

func GetKafkaSyncProducer(name string) *SyncProducer {
	if producer, ok := KafkaSyncProducers[name]; ok {
		return producer
	} else {
		KafkaStdLogger.Println("InitSyncKafkaProducer must be called !")
		return nil
	}
}

func GetKafkaAsyncProducer(name string) *AsyncProducer {
	if producer, ok := KafkaAsyncProducers[name]; ok {
		return producer
	} else {
		KafkaStdLogger.Println("InitAsyncKafkaProducer must be called !")
		return nil
	}
}

func (syncProducer *SyncProducer) keepConnect() {
	defer func() {
		KafkaStdLogger.Println("syncProducer keepConnect exited")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if syncProducer.Status == KafkaProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			syncProducer.Status = KafkaProducerClosed
			syncProducer.StatusLock.Unlock()
			return
		case <-syncProducer.ReConnect:
			if syncProducer.Status != KafkaProducerDisconnected {
				break
			}
			KafkaStdLogger.Println("kafka syncProducer ReConnecting... name " + syncProducer.Name)
			var producer sarama.SyncProducer
		syncBreakLoop:
			for {
				//利用熔断器给集群以恢复时间，避免不断的发送重联
				err := syncProducer.Breaker.Run(func() (err error) {
					producer, err = sarama.NewSyncProducer(syncProducer.Hosts, syncProducer.Config)
					return
				})
				switch err {
				case nil:
					syncProducer.StatusLock.Lock()
					if syncProducer.Status == KafkaProducerDisconnected {
						syncProducer.SyncProducer = &producer
						syncProducer.Status = KafkaProducerConnected
					}
					syncProducer.StatusLock.Unlock()
					KafkaStdLogger.Println("kafka syncProducer ReConnected, name:", syncProducer.Name)
					break syncBreakLoop
				case breaker.ErrBreakerOpen:
					KafkaStdLogger.Println("kafka connect fail, broker is open")
					//2s后重连，此时breaker刚好 half close
					if syncProducer.Status == KafkaProducerDisconnected {
						time.AfterFunc(2*time.Second, func() {
							KafkaStdLogger.Println("kafka begin to ReConnect ,because of  ErrBreakerOpen ")
							syncProducer.ReConnect <- true
						})
					}
					break syncBreakLoop
				default:
					KafkaStdLogger.Println("kafka ReConnect error, name:", syncProducer.Name, err)
				}
			}
		}
	}
}

// 同步生产者状态检查
func (syncProducer *SyncProducer) check() {
	defer func() {
		KafkaStdLogger.Println("syncProducer check exited")
	}()
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if syncProducer.Status == KafkaProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			syncProducer.Status = KafkaProducerClosed
			syncProducer.StatusLock.Unlock()
			return
		}
	}
}

func (asyncProducer *AsyncProducer) keepConnect() {
	defer func() {
		KafkaStdLogger.Println("asyncProducer keepConnect exited")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if asyncProducer.Status == KafkaProducerClosed {
			return
		}
		select {
		case s := <-signals:
			KafkaStdLogger.Println("Kafka async producer receive system signal:" + s.String() + "; name:" + asyncProducer.Name)
			asyncProducer.Status = KafkaProducerClosed
			return
		case <-asyncProducer.ReConnect:
			if asyncProducer.Status != KafkaProducerDisconnected {
				break
			}
			KafkaStdLogger.Println("kafka asyncProducer ReConnecting... name " + asyncProducer.Name)
			var producer sarama.AsyncProducer
		asyncBreakLoop:
			for {
				//利用熔断器给集群以恢复时间，避免不断的发送重联
				err := asyncProducer.Breaker.Run(func() (err error) {
					producer, err = sarama.NewAsyncProducer(asyncProducer.Hosts, asyncProducer.Config)
					return
				})
				switch {
				case err == nil:
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == KafkaProducerDisconnected {
						asyncProducer.AsyncProducer = &producer
						asyncProducer.Status = KafkaProducerConnected
					}
					asyncProducer.StatusLock.Unlock()
					KafkaStdLogger.Println("kafka syncProducer ReConnected, name:", asyncProducer.Name)
					break asyncBreakLoop
				case errors.Is(err, breaker.ErrBreakerOpen):
					KafkaStdLogger.Println("kafka connect fail, broker is open")
					//2s后重连，此时breaker刚好 half close
					if asyncProducer.Status == KafkaProducerDisconnected {
						time.AfterFunc(2*time.Second, func() {
							KafkaStdLogger.Println("kafka begin to ReConnect ,because of  ErrBreakerOpen ")
							asyncProducer.ReConnect <- true
						})
					}
					break asyncBreakLoop
				default:
					KafkaStdLogger.Println("kafka ReConnect error, name:", asyncProducer.Name, err)
				}
			}
		}
	}
}

func (asyncProducer *AsyncProducer) check() {
	defer func() {
		KafkaStdLogger.Println("asyncProducer check exited")
	}()

	for {
		switch asyncProducer.Status {
		case KafkaProducerDisconnected:
			time.Sleep(time.Second * 5)
			continue
		case KafkaProducerClosed:
			return
		}
		// Trap SIGINT to trigger a shutdown.
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		for {
			select {
			case msg := <-(*asyncProducer.AsyncProducer).Successes():
				// TODO 日志打印
				fmt.Println("Success produce message", msg.Topic, msg.Value)
			case err := <-(*asyncProducer.AsyncProducer).Errors():
				if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
					// 连接中断触发重连，捕捉不到 EOF
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == KafkaProducerConnected {
						asyncProducer.Status = KafkaProducerDisconnected
						asyncProducer.ReConnect <- true
					}
					asyncProducer.StatusLock.Unlock()
				}
			case s := <-signals:
				KafkaStdLogger.Println("kafka async producer receive system signal:" + s.String() + "; name:" + asyncProducer.Name)
				asyncProducer.Status = KafkaProducerClosed
				return
			}
		}

	}
}

// SendMessages 同步批量发送消息到kafka
func (syncProducer *SyncProducer) SendMessages(msgs []*sarama.ProducerMessage) (errs sarama.ProducerErrors) {
	if syncProducer.Status != KafkaProducerConnected {
		return append(errs, &sarama.ProducerError{Err: errors.New("kafka syncProducer " + syncProducer.Status)})
	}
	errors.As((*syncProducer.SyncProducer).SendMessages(msgs), &errs)
	for _, err := range errs {
		//触发重连
		if errors.Is(err, sarama.ErrBrokerNotAvailable) {
			syncProducer.StatusLock.Lock()
			if syncProducer.Status == KafkaProducerConnected {
				syncProducer.Status = KafkaProducerDisconnected
				syncProducer.ReConnect <- true
			}
			syncProducer.StatusLock.Unlock()
		}
	}
	return
}

// Send 同步发送消息到 kafka
func (syncProducer *SyncProducer) Send(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if syncProducer.Status != KafkaProducerConnected {
		return -1, -1, errors.New("kafka syncProducer now is " + syncProducer.Status)
	}
	// 分区 , 偏移
	partition, offset, err = (*syncProducer.SyncProducer).SendMessage(msg)
	if err == nil {
		return
	}
	if errors.Is(err, sarama.ErrBrokerNotAvailable) {
		syncProducer.StatusLock.Lock()
		if syncProducer.Status == KafkaProducerConnected {
			syncProducer.Status = KafkaProducerDisconnected
			syncProducer.ReConnect <- true
		}
		syncProducer.StatusLock.Unlock()
	}
	return
}

func (asyncProducer *AsyncProducer) Send(msg *sarama.ProducerMessage) error {
	var err error
	if asyncProducer.Status != KafkaProducerConnected {
		return errors.New("kafka disconneted")
	}
	(*asyncProducer.AsyncProducer).Input() <- msg
	return err
}
func (syncProducer *SyncProducer) Close() error {
	syncProducer.StatusLock.Lock()
	defer syncProducer.StatusLock.Unlock()
	err := (*syncProducer.SyncProducer).Close()
	syncProducer.Status = KafkaProducerClosed
	return err
}

func (asyncProducer *AsyncProducer) Close() error {
	asyncProducer.StatusLock.Lock()
	defer asyncProducer.StatusLock.Unlock()
	err := (*asyncProducer.AsyncProducer).Close()
	asyncProducer.Status = KafkaProducerClosed
	return err
}

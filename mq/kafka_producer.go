package mq

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"go.uber.org/zap"
)

// KafkaProducer 生产者基础结构
type KafkaProducer struct {
	Name       string
	Hosts      []string
	Config     *sarama.Config
	Status     string
	Breaker    *breaker.Breaker
	ReConnect  chan bool
	StatusLock sync.Mutex
	log        Logger // 当前实例使用的 Logger，由 getLogger 解析后绑定
	exit       chan struct{} // 统一退出广播通道
	closed     bool        // Close 已调用标记
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
	syncProducersMu     sync.RWMutex
	KafkaSyncProducers  = make(map[string]*SyncProducer)

	asyncProducersMu    sync.RWMutex
	KafkaAsyncProducers = make(map[string]*AsyncProducer)
)

// endregion

// connectFunc 连接策略函数类型
type connectFunc func() error

// isClosed 返回生产者是否已关闭，调用方必须持有 kp.StatusLock
func (kp *KafkaProducer) isClosed() bool {
	return kp.closed
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
	config.Net.WriteTimeout = time.Second * 30             // 写入超时
	config.Net.ReadTimeout = time.Second * 30              // 读取超时
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

// watchSignals 统一信号监听，收到信号后广播退出
func (kp *KafkaProducer) watchSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	select {
	case <-signals:
		kp.StatusLock.Lock()
		if !kp.isClosed() {
			kp.Status = KafkaProducerClosed
		}
		kp.StatusLock.Unlock()
		close(kp.exit) // 广播退出
	case <-kp.exit:
		// 已经退出（可能通过 Close 触发）
	}
}

// baseKeepConnect 公共重连逻辑，通过策略模式消除重复
func (kp *KafkaProducer) baseKeepConnect(connect connectFunc, typeName string) {
	l := kp.log
	for {
		select {
		case <-kp.exit:
			l.Debug("baseKeepConnect exited", zap.String("name", kp.Name), zap.String("type", typeName))
			return
		case <-kp.ReConnect:
			kp.StatusLock.Lock()
			closed := kp.isClosed()
			disconnected := kp.Status == KafkaProducerDisconnected
			kp.StatusLock.Unlock()
			if closed || !disconnected {
				break
			}
			l.Warn("kafka reconnecting", zap.String("name", kp.Name), zap.String("type", typeName))
		reconnectLoop:
			for {
				err := kp.Breaker.Run(connect)
				switch err {
				case nil:
					l.Info("kafka reconnected", zap.String("name", kp.Name), zap.String("type", typeName))
					break reconnectLoop
				case breaker.ErrBreakerOpen:
					l.Warn("kafka connect fail, breaker is open", zap.String("name", kp.Name))
					kp.StatusLock.Lock()
					disconnected := kp.Status == KafkaProducerDisconnected && !kp.isClosed()
					kp.StatusLock.Unlock()
					if disconnected {
						time.AfterFunc(2*time.Second, func() {
							select {
							case kp.ReConnect <- true:
							case <-kp.exit:
							}
						})
					}
					break reconnectLoop
				default:
					l.Error("kafka reconnect error", zap.String("name", kp.Name), zap.String("type", typeName), zap.Error(err))
				}
			}
		}
	}
}

// syncConnect 同步生产者连接策略
func (sp *SyncProducer) syncConnect() error {
	producer, err := sarama.NewSyncProducer(sp.Hosts, sp.Config)
	if err != nil {
		return err
	}
	sp.StatusLock.Lock()
	if sp.Status == KafkaProducerDisconnected {
		sp.SyncProducer = &producer
		sp.Status = KafkaProducerConnected
	}
	sp.StatusLock.Unlock()
	return nil
}

// asyncConnect 异步生产者连接策略
func (ap *AsyncProducer) asyncConnect() error {
	producer, err := sarama.NewAsyncProducer(ap.Hosts, ap.Config)
	if err != nil {
		return err
	}
	ap.StatusLock.Lock()
	if ap.Status == KafkaProducerDisconnected {
		ap.AsyncProducer = &producer
		ap.Status = KafkaProducerConnected
	}
	ap.StatusLock.Unlock()
	return nil
}

// 初始化同步生产者
// opts 可选：WithLogger(l) 注入文件日志；不传则按 全局SetLogger > 默认控制台 优先级选取
func InitSyncKafkaProducer(name string, hosts []string, config *sarama.Config, opts ...Option) error {
	o := applyOptions(opts)
	syncProducer := &SyncProducer{}
	syncProducer.Name = name
	syncProducer.Hosts = hosts
	syncProducer.Status = KafkaProducerDisconnected
	syncProducer.log = getLogger(o.logger)
	syncProducer.exit = make(chan struct{})
	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	syncProducer.Config = config
	if producer, err := sarama.NewSyncProducer(hosts, config); err != nil {
		return fmt.Errorf("创建生产者失败: %w", err)
	} else {
		// 3次失败 → 熔断 → 2秒后半开 → 成功恢复
		syncProducer.Breaker = breaker.New(3, 1, 2*time.Second)
		syncProducer.ReConnect = make(chan bool)
		syncProducer.SyncProducer = &producer
		syncProducer.Status = KafkaProducerConnected
		// 统一信号监听
		go syncProducer.watchSignals()
		// 断开重连
		go syncProducer.baseKeepConnect(syncProducer.syncConnect, "sync")
		syncProducer.log.Info("SyncKafkaProducer connected", zap.String("name", name))
	}
	syncProducersMu.Lock()
	KafkaSyncProducers[name] = syncProducer
	syncProducersMu.Unlock()
	return nil
}

// 初始化异步生产者
// opts 可选：WithLogger(l) 注入文件日志；不传则按 全局SetLogger > 默认控制台 优先级选取
func InitAsyncKafkaProducer(name string, hosts []string, config *sarama.Config, opts ...Option) error {
	o := applyOptions(opts)
	asyncProducer := &AsyncProducer{}
	asyncProducer.Name = name
	asyncProducer.Hosts = hosts
	asyncProducer.Status = KafkaProducerDisconnected
	asyncProducer.log = getLogger(o.logger)
	asyncProducer.exit = make(chan struct{})
	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	asyncProducer.Config = config
	if producer, err := sarama.NewAsyncProducer(hosts, config); err != nil {
		return fmt.Errorf("创建生产者失败: %w", err)
	} else {
		asyncProducer.Breaker = breaker.New(3, 1, 2*time.Second)
		asyncProducer.ReConnect = make(chan bool)
		asyncProducer.AsyncProducer = &producer
		asyncProducer.Status = KafkaProducerConnected
		// 统一信号监听
		go asyncProducer.watchSignals()
		// 断开重连
		go asyncProducer.baseKeepConnect(asyncProducer.asyncConnect, "async")
		// 异步结果收集
		go asyncProducer.check()
		asyncProducer.log.Info("AsyncKafkaProducer connected", zap.String("name", name))
	}
	asyncProducersMu.Lock()
	KafkaAsyncProducers[name] = asyncProducer
	asyncProducersMu.Unlock()
	return nil
}

func GetKafkaSyncProducer(name string) *SyncProducer {
	syncProducersMu.RLock()
	producer, ok := KafkaSyncProducers[name]
	syncProducersMu.RUnlock()
	if ok {
		return producer
	}
	getLogger(nil).Warn("InitSyncKafkaProducer must be called before GetKafkaSyncProducer",
		zap.String("name", name))
	return nil
}

func GetKafkaAsyncProducer(name string) *AsyncProducer {
	asyncProducersMu.RLock()
	producer, ok := KafkaAsyncProducers[name]
	asyncProducersMu.RUnlock()
	if ok {
		return producer
	}
	getLogger(nil).Warn("InitAsyncKafkaProducer must be called before GetKafkaAsyncProducer",
		zap.String("name", name))
	return nil
}

// check 异步生产者结果收集器，监听成功/失败事件并处理断连检测
func (ap *AsyncProducer) check() {
	l := ap.log
	defer func() {
		l.Debug("asyncProducer check exited", zap.String("name", ap.Name))
	}()
	for {
		ap.StatusLock.Lock()
		closed := ap.isClosed()
		ap.StatusLock.Unlock()
		if closed {
			return
		}
		switch ap.Status {
		case KafkaProducerDisconnected:
			select {
			case <-time.After(5 * time.Second):
			case <-ap.exit:
				return
			}
			continue
		case KafkaProducerClosed:
			return
		}
		select {
		case <-ap.exit:
			return
		case msg := <-(*ap.AsyncProducer).Successes():
			l.Debug("async produce message success",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset))
		case err := <-(*ap.AsyncProducer).Errors():
			if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
				// 先加锁修改状态，再释放锁后发送信号，避免死锁
				ap.StatusLock.Lock()
				shouldReconnect := ap.Status == KafkaProducerConnected
				if shouldReconnect {
					ap.Status = KafkaProducerDisconnected
				}
				ap.StatusLock.Unlock()
				if shouldReconnect {
					select {
					case ap.ReConnect <- true:
					case <-ap.exit:
					}
				}
			} else {
				l.Error("async produce message error",
					zap.String("name", ap.Name), zap.Error(err.Err))
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
		// 触发重连
		if errors.Is(err, sarama.ErrBrokerNotAvailable) {
			// 先加锁修改状态，再释放锁后发送信号，避免死锁
			syncProducer.StatusLock.Lock()
			shouldReconnect := syncProducer.Status == KafkaProducerConnected
			if shouldReconnect {
				syncProducer.Status = KafkaProducerDisconnected
			}
			syncProducer.StatusLock.Unlock()
			if shouldReconnect {
				select {
				case syncProducer.ReConnect <- true:
				case <-syncProducer.exit:
				}
			}
		}
		syncProducer.log.Error("syncProducer sendMessages error",
			zap.String("name", syncProducer.Name), zap.Error(err.Err))
	}
	return
}

// Send 同步发送消息到 kafka
func (syncProducer *SyncProducer) Send(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if syncProducer.Status != KafkaProducerConnected {
		return -1, -1, errors.New("kafka syncProducer now is " + syncProducer.Status)
	}
	// 分区, 偏移
	partition, offset, err = (*syncProducer.SyncProducer).SendMessage(msg)
	if err == nil {
		return
	}
	syncProducer.log.Error("syncProducer send error",
		zap.String("name", syncProducer.Name),
		zap.String("topic", msg.Topic),
		zap.Error(err))
	if errors.Is(err, sarama.ErrBrokerNotAvailable) {
		// 先加锁修改状态，再释放锁后发送信号，避免死锁
		syncProducer.StatusLock.Lock()
		shouldReconnect := syncProducer.Status == KafkaProducerConnected
		if shouldReconnect {
			syncProducer.Status = KafkaProducerDisconnected
		}
		syncProducer.StatusLock.Unlock()
		if shouldReconnect {
			select {
			case syncProducer.ReConnect <- true:
			case <-syncProducer.exit:
			}
		}
	}
	return
}

func (asyncProducer *AsyncProducer) Send(msg *sarama.ProducerMessage) error {
	asyncProducer.StatusLock.Lock()
	status := asyncProducer.Status
	closed := asyncProducer.closed
	asyncProducer.StatusLock.Unlock()

	if closed {
		return errors.New("kafka async producer closed")
	}
	if status != KafkaProducerConnected {
		return errors.New("kafka async producer disconnected")
	}
	select {
	case (*asyncProducer.AsyncProducer).Input() <- msg:
		return nil
	case <-asyncProducer.exit:
		return errors.New("kafka async producer exiting")
	}
}

func (syncProducer *SyncProducer) Close() error {
	syncProducer.StatusLock.Lock()
	defer syncProducer.StatusLock.Unlock()
	if syncProducer.isClosed() {
		return nil
	}
	syncProducer.closed = true
	// 广播退出，停止 watchSignals / baseKeepConnect 等 goroutine
	close(syncProducer.exit)
	syncProducer.Status = KafkaProducerClosed
	return (*syncProducer.SyncProducer).Close()
}

func (asyncProducer *AsyncProducer) Close() error {
	asyncProducer.StatusLock.Lock()
	defer asyncProducer.StatusLock.Unlock()
	if asyncProducer.isClosed() {
		return nil
	}
	asyncProducer.closed = true
	// 广播退出，停止 watchSignals / baseKeepConnect / check 等 goroutine
	close(asyncProducer.exit)
	asyncProducer.Status = KafkaProducerClosed
	return (*asyncProducer.AsyncProducer).Close()
}

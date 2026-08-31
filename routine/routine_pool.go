package routine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HeRedBo/pkg/errors"
	"github.com/HeRedBo/pkg/logx"
)

// option 协程池配置项
type option struct {
	logger logx.Logger
}

// Option 函数式选项
type Option func(*option)

// WithLogger 通过 Option 注入 Logger（优先级最高）
func WithLogger(l logx.Logger) Option {
	return func(opt *option) {
		opt.logger = l
	}
}

var defaultPool *Pool

// 任务接口定义
type Task interface {
	GetTaskName() string
	Execute()
}

// region 函数式任务实现

type Function func()

func (f Function) GetTaskName() string {
	return "unkonwn"
}

func (f Function) Execute() {
	f()
}

// endregion

type BaseTask struct {
	Name string
	F    Function
}

func (t *BaseTask) GetTaskName() string {
	return t.Name
}

func (t *BaseTask) Execute() {
	t.F()
}

// Init 初始化全局协程池
func Init(numWorkers int, maxJobQueueLen int, maxJobTimeout time.Duration) {
	defaultPool = InitPoolWithName("default", numWorkers, maxJobQueueLen, maxJobTimeout)
	defaultPool.Start()
}

func QueueLen() int {
	if defaultPool == nil {
		return 0
	}
	return defaultPool.QueueLen()
}

// region 任务提交接口

func PutTask(f Function) {
	if defaultPool == nil {
		Init(8, 64, 10*time.Second)
	}
	defaultPool.Put(f)
}

func Stop() {
	if defaultPool == nil {
		return
	}
	defaultPool.Stop()
	defaultPool = nil
}

type worker struct {
	Stop chan bool
	Done int64
}

type Pool struct {
	Name           string
	JobQueue       chan Task
	workers        []*worker
	numWorkers     int
	maxJobTimeout  time.Duration
	wg             sync.WaitGroup
	currGorountine int64
	exit           chan bool
	stopping       bool
	running        bool
	log            logx.Logger
}

func InitPoolWithName(name string, numWorkers int, maxJobQueuelen int, maxJobTimeout time.Duration, opts ...Option) *Pool {
	o := &option{}
	for _, opt := range opts {
		opt(o)
	}
	p := &Pool{
		Name:          name,
		JobQueue:      make(chan Task, maxJobQueuelen),
		workers:       make([]*worker, numWorkers),
		numWorkers:    numWorkers,
		maxJobTimeout: maxJobTimeout,
		exit:          make(chan bool, 1),
		log:           getLogger(o.logger),
	}
	for i := 0; i < numWorkers; i++ {
		p.workers[i] = &worker{make(chan bool, 1), 0}
	}
	return p
}

func NewPool(numWorkers int, maxJobQueuelen int, maxJobTimeout time.Duration, opts ...Option) *Pool {
	return InitPoolWithName("default", numWorkers, maxJobQueuelen, maxJobTimeout, opts...)
}

func (p *Pool) QueueLen() int {
	return len(p.JobQueue)
}

func (p *Pool) PutWithTaskName(task *BaseTask) bool {
	return p.put(task)
}

func (p *Pool) Put(f Function) bool {
	return p.put(f)
}

func (p *Pool) PutWait(f Function) {
	if p.stopping {
		p.log.Warn("routinepool was stopping, can not PutWait(task).",
			Field("pool", p.Name))
		return
	}
	p.JobQueue <- f
}

func (p *Pool) put(task Task) bool {
	if p.stopping {
		p.log.Warn("routinepool was stopping, can not put(task).",
			Field("pool", p.Name))
		return false
	}
	p.checkRunningPanic()
	select {
	case p.JobQueue <- task:
		return true
	default:
		p.log.Warn("routinepool Put queue is overflowing.",
			Field("pool", p.Name), Field("cap", cap(p.JobQueue)), Field("len", p.QueueLen()))
		return false
	}
}

func (p *Pool) reput(task Task) {
	p.JobQueue <- task
}

func (p *Pool) executeJob(task Task, timeout time.Duration) {
	// 如果 大量的 task 长时间执行不结束，
	// 会积压在内存中，使进程总goroutine积压。
	// 这里处理方式是超过4倍workers数，即重新投递任务。
	if p.currGorountine >= int64(p.numWorkers*4) {
		time.Sleep(3 * time.Second)
		p.reput(task)
		p.log.Warn("routinepool goroutine count exceeded 4x workers, re-putting job.",
			Field("pool", p.Name), Field("numWorkers", p.numWorkers), Field("goroutine", p.currGorountine))
		return
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	atomic.AddInt64(&p.currGorountine, 1)
	go func() {
		defer atomic.AddInt64(&p.currGorountine, -1)
		if timeout > 0 {
			defer cancel()
		}

		// 捕获异常堆栈
		defer func() {
			e := recover()
			if e != nil {
				s := errors.Stack(2)
				p.log.Error("routinepool panic recovered.",
				Field("pool", p.Name), Field("panic", fmt.Sprintf("%v", e)), Field("traceback", string(s)))
				panic(e)
			}
		}()

		start := time.Now()
		task.Execute()
		if timeout > 0 && time.Since(start) > timeout {
			p.log.Warn("Job running timeout.",
				Field("pool", p.Name), Field("limit", timeout), Field("used", time.Since(start)))
		}
	}()

	if timeout > 0 {
		select {
		// timeout时间到了，就会自动ctx.Done()
		case <-ctx.Done():
		}
	}
}

func (p *Pool) Start() {
	if p.running {
		return
	}
	p.running = true
	for i := 0; i < p.numWorkers; i++ {
		go p.run(i)
	}
	time.Sleep(time.Millisecond) //防止start后马上put(task),接着就stop()

}

func (p *Pool) checkRunningPanic() {
	if !p.running {
		msg := fmt.Sprintf("Pool.Start() must be called before run the routinepool[%v].", p.Name)
		p.log.Warn("Pool.Start() must be called before running the routinepool.",
			Field("pool", p.Name))
		panic(msg)
	}
}

func (p *Pool) run(n int) {
	// p.log.Info("worker start loop.", Field("worker", n))
	defer p.log.Info("routinepool worker exit loop.",
		Field("pool", p.Name), Field("worker", n), Field("done", p.workers[n].Done), Field("queueLen", p.QueueLen()))

	defer p.wg.Done()
	p.wg.Add(1)
	worker := p.workers[n]
	var stop bool = false
	var stopTime time.Time

	for {
		select {
		case task := <-p.JobQueue:
			p.executeJob(task, p.maxJobTimeout)
			worker.Done += 1
		case stop = <-worker.Stop:
			p.log.Info("routinepool worker stop signal.",
				Field("pool", p.Name), Field("worker", n), Field("stop", stop))
			stopTime = time.Now()
			if !stop {
				close(worker.Stop)
			}
			break
		}

		if stop {
			if p.QueueLen() == 0 {
				p.log.Info("worker exit finished.",
					Field("worker", n), Field("goroutine", p.currGorountine))
				break
			}

			if time.Since(stopTime) >= p.maxJobTimeout {
				p.log.Warn("Exit timeout, jobs not finished.",
					Field("timeout", time.Since(stopTime)), Field("remaining", p.QueueLen()))
				break
			} else {
				p.log.Info("Worker exiting, jobs still in queue.",
					Field("worker", n), Field("queueLen", p.QueueLen()), Field("remaining", p.maxJobTimeout-time.Since(stopTime)))
			}
		}
	}
}

func (p *Pool) Stop() {
	p.checkRunningPanic()
	p.stopping = true
	for i := 0; i < p.numWorkers; i++ {
		p.workers[i].Stop <- true
	}
	close(p.exit)
	p.wg.Wait()
	if p.QueueLen() > 0 {
		p.log.Info("routinepool stopped with unfinished jobs.",
			Field("pool", p.Name), Field("remaining", p.QueueLen()))
	}
	var done int64 = 0
	for i := 0; i < p.numWorkers; i++ {
		done += p.workers[i].Done
	}
	p.log.Info("Stop routine pool.",
		Field("pool", p.Name), Field("goroutine", p.currGorountine), Field("queueLen", p.QueueLen()))
}

func (p *Pool) StopWait() {
	p.checkRunningPanic()
	p.stopping = true
	for p.QueueLen() > 0 || p.currGorountine > 0 {
		var done int64 = 0
		for i := 0; i < p.numWorkers; i++ {
			done += p.workers[i].Done
			//p.log.Info("Pool.Stop called.", Field("worker", i))
		}
		p.log.Info("StopWait progress.",
			Field("pool", p.Name), Field("goroutine", p.currGorountine), Field("done", done), Field("queueLen", p.QueueLen()))
		time.Sleep(time.Second * 1)
	}
	p.Stop()
}

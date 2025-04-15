package routine

import (
	"context"
	"fmt"
	"log"
	"os"
	"pkp/errors"
	"sync"
	"sync/atomic"
	"time"
)

// region 定义日志接口
type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// endregion

// 初始化默认日志器
var routineLogger stdLogger

func init() {
	routineLogger = log.New(os.Stdout, "[Routine] ", log.LstdFlags|log.Lshortfile)
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

}

// region 任务提交接口

func PutTask(f Function) {

}

// endregion

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
}

func InitPoolWithName(name string, numWorkers int, maxJobQueuelen int, maxJobTimeout time.Duration) *Pool {
	p := &Pool{
		Name:          name,
		JobQueue:      make(chan Task, maxJobQueuelen),
		workers:       make([]*worker, numWorkers),
		numWorkers:    numWorkers,
		maxJobTimeout: maxJobTimeout,
		exit:          make(chan bool, 1),
	}
	for i := 0; i < numWorkers; i++ {
		p.workers[i] = &worker{make(chan bool, 1), 0}
	}
	return p
}

func NewPool(numWorkers int, maxJobQueuelen int, maxJobTimeout time.Duration) *Pool {
	return InitPoolWithName("default", numWorkers, maxJobQueuelen, maxJobTimeout)
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
		routineLogger.Printf("routinepool[%v] was stopping, can not PutWait(task).", p.Name)
		return
	}
	p.JobQueue <- f
}

func (p *Pool) put(task Task) bool {
	if p.stopping {
		routineLogger.Printf("routinepool[%v] was stopping, can not put(task).", p.Name)
		return false
	}
	p.checkRunningPanic()
	select {
	case p.JobQueue <- task:
		return true
	default:
		routineLogger.Print("routinepool Put(%s) queue.cap=[%v],len=[%v] is overflowing.",
			p.Name, cap(p.JobQueue), p.QueueLen())
		return false
	}
}

func (p *Pool) executeJob(task Task, timeout time.Duration) {
	// 如果 大量的 task 长时间执行不结束，
	// 会积压在内存中，使进程总goroutine积压。
	// 这里处理方式是超过4倍workers数，即重新投递任务。
	if p.currGorountine >= int64(p.numWorkers*4) {
		time.Sleep(3 * time.Second)
		p.reput(task)
		routineLogger.Printf("routinepool[%s] numWorkers=[%v] but gorountine[%v] was running, re-put the job.",
			p.Name, p.numWorkers, p.currGorountine)
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
				log.Fatalf("routinepool[%v] Panic: %v\nTraceback\r:%s",
					p.Name, e, string(s))
			}
		}()

	}()
}

func (p *Pool) reput(task Task) {
	p.JobQueue <- task
}
func (p *Pool) checkRunningPanic() {
	if !p.running {
		msg := fmt.Sprintf("Pool.Start() must be called before run the routinepool[%v].", p.Name)
		routineLogger.Printf(msg)
		panic(msg)
	}

}

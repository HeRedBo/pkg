package routine

import (
	"log"
	"os"
	"time"
)

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var routineLogger stdLogger

func init() {
	routineLogger = log.New(os.Stdout, "[Routine] ", log.LstdFlags|log.Lshortfile)
}

type Task interface {
	GetTaskName() string
	Execute()
}

type Function func()

func (f Function) GetTaskName() string {
	return "unkonwn"
}

func (f Function) Execute() {
	f()
}

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

func Init(numWorkers int, maxJobQueueLen int, maxJobTimeout time.Duration) {

}

func PutTask(f Function) {
}

type worker struct {
	Stop chan bool
	Done int64
}

type Pool struct {
	Name string
	//JobQueue chan
}

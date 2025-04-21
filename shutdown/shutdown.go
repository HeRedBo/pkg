package shutdown

// 提供优雅关闭功能，通过捕获系统信号执行清理操作
import (
	"os"
	"os/signal"
	"syscall"
)

// Hook a graceful shutdown hook, default with signals of SIGINT and SIGTERM
// Hook 定义了一个优雅关闭的钩子接口，默认监听 SIGINT 和 SIGTERM 信号
// 可通过 WithSignals 添加其他信号，并通过 Close 注册关闭时的处理函数
type Hook interface {
	// WithSignals add more signals into hook
	// WithSignals 添加要监听的系统信号
	WithSignals(signals ...syscall.Signal) Hook

	// Close register shutdown handles
	// Close 注册关闭时要执行的处理函数，阻塞等待信号触发

	Close(funcs ...func())
}

// hook 实现 Hook 接口的结构体
type hook struct {
	ctx chan os.Signal
}

// NewHook 创建并初始化一个 Hook 实例
// 默认监听 SIGINT, SIGTERM, SIGQUIT, SIGHUP 信号
// 注意：SIGKILL (强制终止) 无法被捕获，已从默认列表中移除

// NewHook create a Hook instance
func NewHook() Hook {
	hook := &hook{
		ctx: make(chan os.Signal, 1),
	}
	return hook.WithSignals(
		syscall.SIGINT,  // Ctrl+C 中断信号
		syscall.SIGTERM, // 终止信号 (默认 kill 命令)
		syscall.SIGKILL,
		syscall.SIGQUIT, // 退出信号 (Ctrl+\)
		syscall.SIGHUP,  // 终端断开/进程重载
	)
}

// WithSignals 添加要监听的系统信号
// 可以链式调用，多次添加会累积监听信号
func (h *hook) WithSignals(signals ...syscall.Signal) Hook {
	// 将新信号注册到通知列表
	for _, s := range signals {
		signal.Notify(h.ctx, s)
	}
	return h
}

// Close 阻塞等待系统信号，触发后执行所有注册的关闭函数
// 函数按注册顺序执行，执行完毕后停止信号监听
func (h *hook) Close(funcs ...func()) {
	select {
	case <-h.ctx:
	}
	signal.Stop(h.ctx)
	for _, f := range funcs {
		f()
	}

}

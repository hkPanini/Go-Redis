package tcp

import (
	"context"
	"go-redis/interface/tcp"
	"go-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// 启动 TCP server 的相关配置
type Config struct {
	Address string
}

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal) // 用于传输系统的信号
	// 捕获指定的操作系统信号，并通过 Go 通道（sigChan）将这些信号传递给程序，从而实现优雅关闭或动态配置重载等功能
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info("start listen")
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 如果收到外部终止信号（如用户主动关闭），关闭前系统会传递一个信号，告知 listener 和 handler 关闭
	go func() {
		<-closeChan
		logger.Info("shutting down")
		_ = listener.Close()
		_ = handler.Close()
	}()

	// 函数正常退出时（如 return）前要关闭 listener 和 handler
	// 但是 defer 无法响应外部信号：如用户按 Ctrl+C、系统发送 SIGTERM 信号，或通过管理接口触发关闭。
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()
	// 创建一个空的、非派生的顶级上下文（Context），作为处理每个连接的初始上下文
	ctx := context.Background()
	// 等待已有的 Goroutine 完成任务后再继续，防止主协程提前退出，导致子协程未执行完
	var waitDone sync.WaitGroup
	for true {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		logger.Info("accepted link")
		// 使等待队列 + 1
		waitDone.Add(1)
		// 拉起一个新的协程，为每个新建立的网络连接启动一个独立的 Goroutine 来处理请求
		go func() {
			defer func() {
				waitDone.Done()
			}() // 如果直接把 Done 写在 handler.Handle(ctx, conn)后，如果 handler.Handle(ctx, conn)中出错就跑不到Done
			handler.Handle(ctx, conn)
		}() // 此处的()表示调用该匿名函数，即立即执行该函数
	}
	waitDone.Wait()
}

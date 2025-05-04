package tcp

import (
	"bufio"
	"context"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

// 客户端
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait // 采用自己实现的 WaitGroup，具备超时功能，若超时则不再等待，无需等待 Done。
}

func (e *EchoClient) Close() error { // 客户端关闭
	e.Waiting.WaitWithTimeout(10 * time.Second) // 10s 内服务端还没处理好，就超时关闭
	_ = e.Conn.Close()
	return nil
}

type EchoHandler struct {
	activeConn sync.Map       // 当前存活连接，sync.Map 的主要作用就是实现并发安全
	closing    atomic.Boolean // 服务是否正在关闭，如果正在关闭就不接收消息
}

// 构造函数，创建一个 EchoHandler 结构体的新实例并返回其指针
func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

// 服务端业务处理逻辑
func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if handler.closing.Get() { // 如果当前服务正在关闭，则关闭当前客户端发来的连接
		_ = conn.Close()
	}
	client := &EchoClient{
		Conn: conn,
		// Waiting 不用初始化，其底层是一个 WaitGroup，会自动为 0。
	}
	handler.activeConn.Store(client, struct{}{}) // 存储客户端信息，只关心键的存在性，使用空结构体不需要存储实际值

	// 服务端业务开始：不断读取客户端发来的数据，并将发来的数据回发（以 \n 为界）
	reader := bufio.NewReader(conn) // 用于读取网络或 I/O 数据，为底层连接（如 net.Conn）包装一个带缓冲的读取器。
	for {
		msg, err := reader.ReadString('\n') // 从缓冲读取器中持续读取数据，直到遇到换行符 \n 或发生错误
		if err != nil {
			if err == io.EOF { // 客户端退出
				logger.Info("Connection close")
				handler.activeConn.Delete(client) // 删除服务端中 sync.map 中存储的对应的的客户端信息
			} else {
				logger.Warn(err)
			}
			return
		}
		client.Waiting.Add(1) // waitGroup +1
		b := []byte(msg)      // msg 转换为字节流
		_, _ = conn.Write(b)
		client.Waiting.Done()
	}
}

// 服务端关闭
func (handler *EchoHandler) Close() error {
	logger.Info("handler shutting down")
	handler.closing.Set(true) // 将服务端 closing 的状态设置为 true
	handler.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*EchoClient) // key 原来的类型是 any，即空接口，需要做类型断言
		client.Conn.Close()
		return true // 继续遍历下一个键值对, 若返回 false 会中止遍历
	})
	return nil
}

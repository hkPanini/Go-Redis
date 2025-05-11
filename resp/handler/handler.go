package handler

import (
	"context"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type RespHandler struct {
	activeConn sync.Map
	db         databaseface.Database // db 实际上是一个接口，可以进行不同的实现
	closing    atomic.Boolean        // 标识是否正在关闭中
}

func (r *RespHandler) MakeHandler() *RespHandler {
	var db databaseface.Database
	db = database.NewEchoDatabase()
	return &RespHandler{
		db: db,
	}
}

// closeClient 关闭某个客户端的连接

func (r *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	r.db.AfterClientClose(client)
	r.activeConn.Delete(client)
}

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if r.closing.Get() {
		_ = conn.Close()
	}
	client := connection.NewConn(conn)
	r.activeConn.Store(client, struct{}{})
	ch := parser.ParseStream(conn) // 开始处理解析连接发来的数据
	for payload := range ch {
		// 出错
		if payload.Err != nil {
			// 用户正在四次挥手关闭连接
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				// 使用了一个已关闭的连接
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// 一般的协议出错，回写错误
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			// 回写错误时出错了
			if err != nil {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		// 正常执行
		if payload.Data == nil {
			continue
		}
		reply, ok := payload.Data.(*reply.MultiBulkReply) // 类型断言，因为 Exec 需要的是 Args [][]byte
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := r.db.Exec(client, reply.Args) // 让 redis 内核去执行该条解析出来的指令
		if result != nil {                      // 解析结果不为空
			_ = client.Write(result.ToBytes())
		} else { // 解析结果为空
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close 关闭 handler 及所有连接

func (r *RespHandler) Close() error {
	logger.Info("handler shutting down")
	r.closing.Set(true)
	r.activeConn.Range(
		func(key, value interface{}) bool {
			client := key.(*connection.Connection)
			_ = client.Close()
			return true // return true，Range 才会遍历下一个 k, v
		},
	)
	r.db.Close()
	return nil
}

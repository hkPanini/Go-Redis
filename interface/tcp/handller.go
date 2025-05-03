package tcp

import (
	"context"
	"net"
)

// 该 Handler 用于实现 TCP 应用服务器
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

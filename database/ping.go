package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}

// 程序启动时将 ping 方法注册到全局方法表中
func init() {
	RegisterCommend("ping", Ping, 1)
}

package database

import "go-redis/interface/resp"

type CmdLine = [][]byte // 指令 args 的别名

type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply // Exec 执行
	Close()
	AfterClientClose(c resp.Connection) // 关闭后的工作，如痕迹抹除
}

// 指代 redis 的数据结构，即 List, string等

type DataEntity struct {
	Data interface{}
}

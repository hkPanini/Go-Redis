package database

import "strings"

// cmdTable 用于记录系统中所有的指令(GET, SET, PING等)与commend结构体的关系，即每一条指令都对应一个commend结构体
var cmdTable = make(map[string]*commend)

// 每一个指令(GET, SET等)都是一个结构体
type commend struct {
	executor ExecFunc // 执行方法
	arity    int      // 参数的数量
}

// RegisterCommend 用于注册一些指令的实现
// 通过输入方法的名称、输入方法的执行函数、输入方法执行需要的参数个数，将上述三个参数封装成一个 commend 结构体，并注册到 cmdTable 中

func RegisterCommend(name string, executor ExecFunc, arity int) {
	name = strings.ToLower(name) // 转化成小写
	cmdTable[name] = &commend{
		executor: executor,
		arity:    arity,
	}
}

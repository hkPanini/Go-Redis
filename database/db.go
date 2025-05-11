package database

import (
	"go-redis/datastruct/dict"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

type DB struct {
	index int
	data  dict.Dict
}

type ExecFunc func(db *DB, args [][]byte) resp.Reply // redis 所有指令的函数规范，入参是 db 和指令，出参是 reply

type CmdLine = [][]byte

func MakeDB() *DB {
	db := &DB{
		data: dict.MakeSyncDict(),
	}
	return db
}

func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	// 获取指令的第一个成员（如 SET, SETNX, PING）
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown commend" + cmdName)
	}
	// 校验参数个数是否合法
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	// 获取指令对应的执行方法
	function := cmd.executor
	// cmdLine 的第一个已经使用过了，假设是 set key value，前面的 cmdName 已经取到了 set，因此只需要传递指令剩下的内容即可
	return function(db, cmdLine[1:])
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	return true
}

package database

import (
	"go-redis/datastruct/dict"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

type DB struct {
	index  int
	data   dict.Dict
	addAof func(line CmdLine)
}

type ExecFunc func(db *DB, args [][]byte) resp.Reply // redis 所有指令的函数规范，入参是 db 和指令，出参是 reply

type CmdLine = [][]byte

func MakeDB() *DB {
	db := &DB{
		data:   dict.MakeSyncDict(),
		addAof: func(line CmdLine) {},
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

// validateArity 用于校验参数个数是否合法
// 当遇到 set key value ，参数数量固定为 3
// 当遇到 exists k1 k2 k3 k4 ... 指令，参数数量不定，当参数个数不定时，在arity前加负号，表示可以超过这个数量，如-3，参数值可以大于 3

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 { // arity >= 0，参数数量固定
		return argNum == arity
	} else { // arity < 0，参数数量不定，但必须大于等于 arity 的绝对值
		return argNum >= -arity
	}
}

// GetEntity 用于到DB中根据 key 取 value(entity)

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key) // raw 原始格式，因为 Get 取出来是一个空接口，后续需要做断言
	if !ok {
		return nil, false
	}
	entity := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity 用于到DB中根据 key 新增/更新 value(entity)

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exist := db.data.Get(key)
		if exist {
			db.Remove(key)
			deleted++
		}
	}
	return
}

func (db *DB) Flush() {
	db.data.Clear()
}

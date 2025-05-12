package database

import (
	"go-redis/interface/resp"
	"go-redis/lib/wildcard"
	"go-redis/resp/reply"
)

// DEL k1, k2, k3

func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
}

// EXISTS K1, K2, K3 ... 查询这些key有几个存在

func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exist := db.GetEntity(key)
		if exist {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

// FLUSHDB 清空数据库

func execFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	return reply.MakeOkReply()
}

// TYPE k1 用于查看键对应的值的数据类型

func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exist := db.GetEntity(key)
	if !exist {
		return reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		reply.MakeStatusReply("string") // 回复键的类型为 string
	}
	// TODO
	return reply.MakeUnknownErrReply()
}

// RENAME k1 k2 用于将 k1 重命名为 k2

func execRename(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])
	entity, exist := db.GetEntity(src)
	if !exist {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Removes(src)
	return reply.MakeOkReply()
}

// RENAMENX k1 k2 用于将 k1 重命名为 k2，如果 k2 是早已存在的，则不执行此次重命名

func execRenameNX(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	_, ok := db.GetEntity(dest)
	if ok { // 如果 k2 原本就存在，则返回 0，表示未进行操作
		return reply.MakeIntReply(0)
	}

	entity, exist := db.GetEntity(src)
	if !exist {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Removes(src)
	return reply.MakeIntReply(1)
}

// KEYS * 列出该DB种所有的 KEY

func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern, _ := wildcard.CompilePattern(string(args[0])) // 根据输入的通配符，选择相应的模式，比如 * 则返回所有内容
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) { // 判断是 key 否符合 pattern
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommend("DEL", execDel, -2)
	RegisterCommend("EXISTS", execExists, -2)
	RegisterCommend("FLUSHDB", execFlushDB, -1)
	RegisterCommend("TYPE", execType, 2)
	RegisterCommend("RENAME", execRename, 3)
	RegisterCommend("RENAMENX", execRenameNX, 3)
	RegisterCommend("KEYS", execKeys, 2) // 第一个参数是 keys，第二个参数是通配符，比如 *
}

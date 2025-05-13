package database

import (
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"strconv"
	"strings"
)

// redis 数据库， 下辖多个子数据库 db

type Database struct {
	dbSet []*DB // 子数据库，默认16个，通过参数 Databases，于 redis.conf 中进行修改
}

func NewDatabase() *Database {
	database := &Database{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	database.dbSet = make([]*DB, config.Properties.Databases)
	for i := range database.dbSet { // 初始化 db
		db := MakeDB()
		db.index = i
		database.dbSet[i] = db
	}
	return database
}

// 根据用户选择的子db，将用户发来的指令发给分db去执行
// set k v, get k, select index ...

func (database *Database) Exec(client resp.Connection, args [][]byte) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	cmdName := strings.ToLower(string(args[0])) // 取出第一个参数，如 get, set 等
	if cmdName == "select" {                    // 当前指令用于选择子数据库
		if len(args) != 2 { // 选择子数据库只用 2 个参数，如 select 10
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(client, database, args[1:])
	}
	// 修改子数据库以外的命令的处理逻辑如下
	dbIndex := client.GetDBIndex()
	db := database.dbSet[dbIndex]
	return db.Exec(client, args)
}

func (database *Database) Close() {}

func (database *Database) AfterClientClose(c resp.Connection) {}

// 用户选择子db
func execSelect(c resp.Connection, database *Database, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil { // 用户输入的子数据库编号不是数字，报错
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(database.dbSet) { // 用户输入的子数据库编号大于子数据库数量，报错
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex) // 更改用户连接中的子数据库编号字段
	return reply.MakeOkReply()
}

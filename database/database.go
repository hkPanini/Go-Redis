package database

import (
	"go-redis/aof"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"strconv"
	"strings"
)

// redis 数据库， 下辖多个子数据库 db

type StandaloneDatabase struct {
	dbSet      []*DB // 子数据库，默认16个，通过参数 Databases，于 redis.conf 中进行修改
	aofHandler *aof.AofHandler
}

func NewDatabase() *StandaloneDatabase {
	database := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	database.dbSet = make([]*DB, config.Properties.Databases)
	// 初始化 db
	for i := range database.dbSet {
		db := MakeDB()
		db.index = i
		database.dbSet[i] = db
	}
	// 初始化 aof
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(database)
		if err != nil {
			panic(err)
		}
		database.aofHandler = aofHandler
		for _, db := range database.dbSet {
			// 定义每个子数据库的 addAof 方法，
			// addAof 和 AddAof 不是同一个方法，而是子数据库db层的addAof去调用Database层的AddAof
			// 实现子数据库db往aof管道中写入指令

			// 注意闭包引发的bug
			// 创建局部变量，固定当前迭代的 db,防止直接引用 db 造成内存逃逸
			// 当循环结束后，所有闭包中的 db 都指向最后一次迭代的db（即第 16 个数据库，索引为 15）。因此调用时所有闭包都会使用索引15
			//建一个中间变量 currentDB
			currentDB := db
			currentDB.addAof = func(line CmdLine) {
				// AddAof(db.index, line) 中的 db 引用了外部 for 中的 db 造成内存逃逸，for 中的 db 变量逃逸到堆上
				// database.aofHandler.AddAof(db.index, line)
				database.aofHandler.AddAof(currentDB.index, line)
			}
		}
	}
	return database
}

// 根据用户选择的子db，将用户发来的指令发给分db去执行
// set k v, get k, select index ...

func (database *StandaloneDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
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

func (database *StandaloneDatabase) Close() {}

func (database *StandaloneDatabase) AfterClientClose(c resp.Connection) {}

// 用户选择子db
func execSelect(c resp.Connection, database *StandaloneDatabase, args [][]byte) resp.Reply {
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

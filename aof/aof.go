package aof

import (
	"go-redis/config"
	"go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
	"os"
	"strconv"
)

type CmdLine = [][]byte

const aofBufferSize = 1 << 16

// 传入 aof 文件的记录，标识对指定db的具体操作
type payload struct {
	cmdline CmdLine
	dbIndex int
}

type AofHandler struct {
	database    database.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	currentDB   int
}

func NewAofHandler(database database.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.database = database
	// 加载 AOF，将历史的AOF内容进行恢复
	handler.LoadAof()
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	// channel 初始化
	handler.aofChan = make(chan *payload, aofBufferSize)
	// 开启协程从管道中取出记录进行落盘
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// 把操作记录写入管道，由管道异步地写入aof文件，避免直接落盘造成的同步阻塞

func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) {
	// 判断是否开启 AOF 功能，以及 aofChan 是否初始化，如果都满足，则将记录写入管道
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdline: cmd,
			dbIndex: dbIndex,
		}
	}
}

// 从管道中取出记录，将记录写入磁盘文件aof中

func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		// 如果发生了db切换，要往aof文件中额外插入一条select语句(*2/r/n$6/r/nselect/r/n$1/r/n3/r/n)，表示选择了某某db
		if p.dbIndex != handler.currentDB {
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
				continue // 继续读下一条
			}
			handler.currentDB = p.dbIndex
		}
		// 未切换db || 切换完成后：将指令按符合resp协议的格式写入aof文件
		data := reply.MakeMultiBulkReply(p.cmdline).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
		}
	}
}

// LoadAof
func (handler *AofHandler) LoadAof() {

}

package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func flushdb(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	replies := cluster.broadcast(c, cmdArgs) // 得到一个 map，“节点” -> “执行结果” 的映射
	var errReply reply.ErrorReply            // 只要有一个节点报错，就认为 flushdb 执行失败
	for _, r := range replies {
		if reply.IsErrorReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil { // errReply 为空说明没有错误
		return reply.MakeOkReply()
	}
	return reply.MakeErrReply("error: " + errReply.Error())
}

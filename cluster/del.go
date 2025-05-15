package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// 删除特殊的地方在于：一个 del 命令后可以跟很多个 key，如 del k1 k2 k3 k4 k5，需要返回真实删除了多少个 key
// del 可以利用广播来实现，类似 rename

func del(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	replies := cluster.broadcast(c, cmdArgs) // 得到一个 map，“节点” -> “执行结果” 的映射
	var errReply reply.ErrorReply            // 只要有一个节点报错，就认为 del 执行失败
	var deleted int64 = 0                    // 用于记录实际删除的 key 的个数
	for _, r := range replies {
		if reply.IsErrorReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
		intReply, ok := r.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}
	if errReply == nil { // errReply 为空说明没有错误
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error: " + errReply.Error())
}

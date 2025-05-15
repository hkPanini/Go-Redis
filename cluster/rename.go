package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// rename k1 k2

func rename(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	if len(cmdArgs) != 3 {
		return reply.MakeErrReply("ERR Wrong number args")
	}
	src := string(cmdArgs[1])                     // 原来 key 的 name
	dest := string(cmdArgs[2])                    // 修改后的 key 的 name
	srcPeer := cluster.peerPicker.PickNode(src)   // 修改前该 key 所在的槽点对应的节点
	destPeer := cluster.peerPicker.PickNode(dest) // 修改后该 key 所在的槽点对应的节点
	// 如果改名前和改名后该key不隶属于同一个节点，则报错。其实也可以实现，在原节点删除该key，然后在新节点新增该key即可。
	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one peer")
	}
	return cluster.relay(srcPeer, c, cmdArgs) // 调用 relay 转发
}

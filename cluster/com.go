package cluster

import (
	"context"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/client"
	"go-redis/resp/reply"
	"strconv"
)

// 从目标连接池中取到一个连接，入参是目标节点的地址

func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := cluster.peerConnection[peer] // 拿到了当前节点维护的目标节点的连接池
	if !ok {
		return nil, errors.New("connection not found")
	}
	// 从连接池中借用一个连接
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("connection wrong type")
	}
	return c, err
}

// 将连接还回连接池

func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	pool, ok := cluster.peerConnection[peer] // 拿到还回的目标连接池
	if !ok {
		return errors.New("connection not found")
	}
	return pool.ReturnObject(context.Background(), peerClient)
}

// 将用户指令（args）通过连接（c）转发给目标节点（peer），并返回 reply

func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self { // 如果 peer 是自己，则直接执行指令
		return cluster.db.Exec(c, args)
	}
	peerClient, err := cluster.getPeerClient(peer) // 从目标连接池中取出一个连接
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient) // 用于及时归还连接池连接
	}()
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex()))) // 转发指令前，先转发“选择子数据库”指令
	return peerClient.Send(args)
}

// 广播转发指令，如对于命令 flushDB，需要广播该指令，令所有 Redis 节点进行数据删除，返回多个 Reply

func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	results := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		result := cluster.relay(node, c, args)
		results[node] = result
	}
	return results
}

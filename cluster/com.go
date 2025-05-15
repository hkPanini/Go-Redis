package cluster

import (
	"context"
	"errors"
	"go-redis/resp/client"
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

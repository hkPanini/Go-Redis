package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/resp/client"
)

type connectionFactory struct {
	Peer string // 该连接池由哪个节点维护，保存该节点的地址
}

// MakeObject 新建客户端连接

func (f connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	c.Start()
	return pool.NewPooledObject(c), nil
}

// DestroyObject 关闭客户端连接

func (f connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	c.Close()
	return nil
}

func (f connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (f connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (f connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/config"
	database2 "go-redis/database"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
)

type ClusterDatabase struct {
	self           string                      // 记录节点自己的名称 or 地址
	nodes          []string                    // 整个集群所有的节点，包括自己
	peerPicker     *consistenthash.NodeMap     // peer 同辈；节点选择器,通过一致性哈希算法选择目标节点
	peerConnection map[string]*pool.ObjectPool // 当前节点会为每个其他节点（如 node-2 和 node-3）维护一个独立的网络连接池，用于高效管理到这些节点的通信连接
	db             database.Database
}

func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,            // 从配置中读取自身地址
		db:             database2.NewStandaloneDatabase(), // 初始化本地数据库
		peerPicker:     consistenthash.NewNodeMap(nil),    // 创建一致性哈希选择器
		peerConnection: make(map[string]*pool.ObjectPool), // 初始化空连接池映射
	}

	// 初始化 nodes
	nodes := make([]string, 0, len(config.Properties.Peers)+1) // len(config.Properties.Peers)+1 意味其余节点和自己
	// 将 self 和 peers 都加入 nodes 中
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...) // 构建一致性哈希环
	cluster.nodes = nodes

	// 初始化连接池 peerConnection
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		pool.NewObjectPoolWithDefaultConfig( // 使用默认配置创建连接池，每个其他节点对应一个连接池，存储在 peerConnection 映射中
			ctx,
			&connectionFactory{Peer: peer})
	}
	return cluster
}

type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply

var router = makeRouter()

func (c *ClusterDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	//TODO implement me
	panic("implement me")
}

func (c *ClusterDatabase) Close() {
	//TODO implement me
	panic("implement me")
}

func (c *ClusterDatabase) AfterClientClose(f resp.Connection) {
	//TODO implement me
	panic("implement me")
}

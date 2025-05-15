package cluster

import "go-redis/interface/resp"

// 指定指令和执行方式（relay or broadcast）的对应，入参是指令的名称，出参是“指令名称” -> “执行方式” 的哈希映射

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["exists"] = defaultFunc
	routerMap["type"] = defaultFunc
	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc
	routerMap["ping"] = ping
	routerMap["rename"] = rename
	routerMap["renamenx"] = rename // 和 rename 一样
	routerMap["flushdb"] = flushdb
	routerMap["del"] = del
	routerMap["select"] = execSelect

	return routerMap
}

// 默认转发方法，走 relay

func defaultFunc(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	key := string(cmdArgs[1])
	peer := cluster.peerPicker.PickNode(key) // 获取到该 key 哈希之后得到的哈希值对应的槽位对应的节点
	return cluster.relay(peer, c, cmdArgs)   // 调用 relay 方法将指令转发到目标节点
}

package consistenthash

import "hash/crc32"

type HashFunc func(data []byte) uint32

// redis 节点

type NodeMap struct {
	hashFunc    HashFunc       // 哈希函数
	nodeHashs   []int          // 该节点负责的槽位
	nodehashMap map[int]string // 槽位 -> 节点的映射，根据该槽位来查由哪个节点负责
}

func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc:    fn,
		nodehashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

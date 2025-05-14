package consistenthash

import (
	"hash/crc32"
	"sort"
)

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

// 某节点负责的槽位是否为空，即判断某节点是否没有被分配槽位

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

// 增加节点，并给该节点分配槽位

func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodehashMap[hash] = key
	}
	sort.Ints(m.nodeHashs)
}

// 将key做哈希映射到槽位上，再按槽位找到该槽位对应的节点，按照 key 查找该 key-value 属于哪个节点

func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() { // 如果该节点没有被分配槽位
		return ""
	}
	hash := int(m.hashFunc([]byte(key))) // 将key做哈希映射
	// 通过二分查找第一个大于等于 key 哈希的槽位，若未找到则取第一个槽位（形成环）
	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	}) // 用 key 的哈希值得到对应的节点序号
	if idx == len(m.nodeHashs) { // 类似取模操作
		idx = 0
	}
	return m.nodehashMap[m.nodeHashs[idx]] // 到 nodehashMap 中得到节点的名称
}

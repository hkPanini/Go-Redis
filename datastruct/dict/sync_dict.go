package dict

import "sync"

// 并发安全的字典

type SyncDict struct {
	m sync.Map
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

func (dict *SyncDict) Get(key string) (val interface{}, exist bool) {
	val, ok := dict.m.Load(key)
	return val, ok
}

func (dict *SyncDict) Len() int {
	length := 0
	dict.m.Range(func(key, value interface{}) bool {
		length++
		return true
	})
	return length
}

func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key) // 查看当前 key 是否存在
	dict.m.Store(key, val)
	if existed {
		return 0 // return 0 是指对已存在的值进行了修改，而不是插入了一个新的值
	}
	return 1 // 插入了一个新的值
}

func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key) // 查看当前 key 是否存在
	if existed {
		return 0 // 如果存在，则不做任何修改
	}
	dict.m.Store(key, val)
	return 1
}

func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key) // 查看当前 key 是否存在
	if existed {
		dict.m.Store(key, val) // 如果存在，则修改其值
		return 1
	}
	return 0
}

func (dict *SyncDict) Remove(key string) (result int) {
	_, existed := dict.m.Load(key) // 查看当前 key 是否存在
	dict.m.Delete(key)
	if existed {
		return 1
	}
	return 0
}

func (dict *SyncDict) ForEach(consumer Consumer) {
	dict.m.Range(func(key, value interface{}) bool {
		consumer(key.(string), value)
		return true
	})
}

func (dict *SyncDict) Keys() []string {
	result := make([]string, dict.Len())
	dict.m.Range(func(key, value interface{}) bool {
		result = append(result, key.(string)) // 随机从一个k, v 开始
		return true                           // 依次往后遍历
	})
	return result
}

func (dict *SyncDict) RandomKeys(limit int) []string {
	result := make([]string, dict.Len())
	for i := 0; i < limit; i++ {
		dict.m.Range(func(key, value interface{}) bool {
			result = append(result, key.(string)) // 随机从一个k, v 开始，
			return false                          // 不往后遍历，所以每一轮只取一个kv，然后又开始下一轮随机选取
		})
	}
	return result
}

func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result = append(result, key.(string)) // 随机从一个k, v 开始，
		i++
		if i == limit { // 达到指定数量后停止往后取 kv
			return false
		}
		return true // 在达到指定数量前依次往后取 kv
	})
	return result
}

func (dict *SyncDict) clear() {
	*dict = *MakeSyncDict() // 直接赋予一个新的 SyncDict，旧的让 go 自动 gc 即可
}

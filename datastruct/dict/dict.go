package dict

type Consumer func(key string, val interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exist bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int) // 如果不存在则 put
	PutIfExists(key string, val interface{}) (result int) // 如果存在则 put
	Remove(key string) (result int)
	ForEach(consumer Consumer)             // 遍历字典
	Keys() []string                        // 列出所有的 key
	RandomKeys(limit int) []string         // 随机返回 limit 个 key，可能有重复
	RandomDistinctKeys(limit int) []string // 随机返回 limit 个 key，不会重复
	clear()                                // 清空字典
}

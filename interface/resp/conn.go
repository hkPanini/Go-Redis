package resp

// Connection 代表与Redis客户端的连接
type Connection interface {
	Write([]byte) error
	GetDBIndex() int // 获取库的index
	SelectDB(int)    // 根据 index 选择库
}

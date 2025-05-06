package reply

// pong 是 ping 的回复
type PongReply struct{}

var pongBytes = []byte("+PONG\r\n")

// 用于对 ping 回复常量 pongbytes
func (r PongReply) ToBytes() []byte {
	return pongBytes
}

// PongReply 的构造函数
func MakePongReply() *PongReply {
	return &PongReply{}
}

// 回复 OK
type OkReply struct{}

var okBytes = []byte("+OK\r\n")

func (r *OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply) // 持有一个初始化的okReply常量，这样初始化时就不用每次都新建一个，节约内存

func MakeOkReply() *OkReply {
	return theOkReply
}

// 空的字符串回复，-1 表示不存在
type NullBulkReply struct{}

var nullBulkBytes = []byte("$-1\r\n") // -1 代表为不存在，0代表""空列表[]。

func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// EmptyMultiBulkReply 表示空的多批量回复（即空列表）
type EmptyMultiBulkReply struct{}

var emptyMultiBulkBytes = []byte("*0\r\n")

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

// NoReply 代表不返回任何响应，适用于订阅类命令（如 SUBSCRIBE）
type NoReply struct{}

var noBytes = []byte("")

func (r *NoReply) ToBytes() []byte {
	return noBytes
}

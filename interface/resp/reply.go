package resp

// Reply是Redis序列化协议（RESP）消息的接口
type Reply interface {
	ToBytes() []byte // 将回复的内容转换为字节
}

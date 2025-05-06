package reply

// 用于表示 错误回复，除了需要实现 Reply 的 ToBytes() 方法，还扩展了 Error() 方法
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

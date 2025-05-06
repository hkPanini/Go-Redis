package reply

// 未知错误回复
type UnknownErrReply struct{}

var unknownErrBytes = []byte("-Err unknown\r\n")

func (u *UnknownErrReply) Error() string {
	return "Err unknown"
}

func (u *UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

func MakeUnknownErrReply() *UnknownErrReply {
	return &UnknownErrReply{}
}

// ArgNumErrReply 表示命令的参数数量错误
type ArgNumErrReply struct {
	Cmd string // 命令名（如 "SET"）
}

func (r *ArgNumErrReply) Error() string {
	return "-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n"
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

// 客户端发送的命令存在语法问题，参数类型不匹配等
type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-Err syntax error\r\n")
var theSyntaxErrReply = &SyntaxErrReply{}

func MakeSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}

func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func (r *SyntaxErrReply) Error() string {
	return "Err syntax error"
}

// WrongTypeErrReply 表示对持有错误类型值的键执行操作，指操作的数据类型与命令不匹配（例如对字符串执行 HGET）
type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

// ToBytes marshals redis.Reply
func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

// ProtocolErrReply 表示请求数据不符合 RESP 格式规范
type ProtocolErrReply struct {
	Msg string
}

func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}

func (r *ProtocolErrReply) Error() string {
	return "ERR Protocol error '" + r.Msg + "' command"
}

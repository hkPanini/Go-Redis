package reply

import (
	"bytes"
	"go-redis/interface/resp"
	"strconv"
)

var (
	CRLF = "\r\n"
)

/* ---- Bulk Reply ---- */

// Bulk String 批量字符串是 Redis 协议（RESP）中的一种回复类型，专门用于传输二进制安全的字符串数据

type BulkReply struct {
	Arg []byte // 存储二进制安全的字符串数据
}

var nullBulkReplyBytes = []byte("$-1")

func (b *BulkReply) ToBytes() []byte {
	if b.Arg == nil { // Arg 为空返回 -1
		return nullBulkReplyBytes
	}
	return []byte("$" + strconv.Itoa(len(b.Arg)) + CRLF + string(b.Arg) + CRLF)
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

/* ---- Multi Bulk Reply ---- */

type MultiBulkReply struct {
	Args [][]byte
}

func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer // 用于拼装 Args [][]byte 中的多个一维字节
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString(string(nullBulkReplyBytes) + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

/* ---- Status Reply ---- */

// Redis 协议（RESP）中的一种回复类型，专门用于传输简单的状态信息，如操作成功提示

type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

/* ---- Int Reply ---- */

// IntReply 是 Redis 协议（RESP）中的一种回复类型，专门用于传输整数类型的响应
type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ---- Error Reply ---- */

// 用于表示 错误回复，除了需要实现 Reply 的 ToBytes() 方法，还扩展了 Error() 方法

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrReply 用于表示服务端产生的通用错误

type StandardErrReply struct {
	Status string
}

func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply 检测一个 Redis 协议回复（resp.Reply）是否属于错误回复（即是否符合 RESP 错误格式）

func IsErrorReply(reply resp.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

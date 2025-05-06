// Parser 负责协议解析（字节流 -> 结构化数据）和序列化（结构化数据 -> 字节流），是 Redis 客户端与服务端通信的翻译器

package parser

import (
	"go-redis/interface/resp"
	"io"
)

// Payload 是解析结果（或错误）的封装容器，用于统一传递解析后的数据或错误信息

type Payload struct {
	Data resp.Reply
	Err  error
}

// readState 是解析器 parser 的状态

type readState struct {
	readingMultiLine  bool     // 标识解析器解析的是单行数据还是多行数据
	expectedArgsCount int      // 标识正在读取的指令期待有多少个参数
	msgType           byte     // 标识消息的类型
	args              [][]byte // 已解析的传来的具体指令
	bulkLen           int64    // 传来的字节组（数据块）的长度
}

// 判断解析器是否完成

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// ParseStream 用于并发解析指令，从 io.Reader 读取数据并通过通道（channel）发送解析后的有效载荷（Payloads）
// ParseStream 首字母大写，用作协议层对外的接口
// ParseStream 创建一个双向通道 ch，但通过返回 <-chan 将其转换为只读通道

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// 指令解析逻辑，解析发来的指令

func parse0(reader io.Reader, ch chan<- *Payload) {

}

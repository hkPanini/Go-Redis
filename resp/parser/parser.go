// Parser 负责协议解析（字节流 -> 结构化数据）和序列化（结构化数据 -> 字节流），是 Redis 客户端与服务端通信的翻译器

package parser

import (
	"bufio"
	"errors"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"io"
	"strconv"
	"strings"
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
	msgType           byte     // 标识消息的类型，* 表示在读一个数组（一个数组中有多条指令），$ 表示在读一条指令
	args              [][]byte // 已解析的传来的具体指令
	bulkLen           int64    // 传来的字节组（数据块）的长度，即 $ 符号后面跟着的数字
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

// readLine 用于读取以 \r\n 结尾的一行指令，只负责读取，不负责任何解析

func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) { // bool 为是否发生I/O错误
	var msg []byte
	var err error

	// 1. 没有读到 $ 指明指令长度时，直接按 \r\n 切分
	if state.bulkLen == 0 {
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error" + string(msg))
		}
		// 2. 读到 $ 时，严格读取相应的字符个数，哪怕遇到 \r\n 也要读入
	} else {
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error" + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

// parseMultiBulkHeader 用于解析处理 readLine 中读取到的 "*<number>/r/n"

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64 // 存储 * 号后的数字，如 *3\r\n$3\r\nSET\r\n$3key\r\n$5\r\nvalue\r\n 中开头的 3, 即后面包含多少个指令
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error" + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]        // 标识为 *，表示在读数组
		state.readingMultiLine = true // 表示在读数组，包含有多个指令
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else { // expectedLine < 0
		return errors.New("protocol error" + string(msg))
	}
}

// $4\r\nPING\r\n

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error" + string(msg))
	}
	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]        // 标识为 $，表示在读数组
		state.readingMultiLine = true // 表示在读数组，包含有多个指令
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error" + string(msg))
	}
}

// parseSingleLineReply 用于处理 "+OK\r\n" 和 "-err\r\n" 和 ":5\r\n" 这三种单行指令

func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n") // 剪切掉 \r\n
	var result resp.Reply
	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error" + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

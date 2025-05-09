// Parser 负责协议解析（字节流 -> 结构化数据）和序列化（结构化数据 -> 字节流），是 Redis 客户端与服务端通信的翻译器

package parser

import (
	"bufio"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"io"
	"runtime/debug"
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
	expectedArgsCount int      // 标识正在读取的数据包含多少个指令，即 * 号后的数字是多少
	msgType           byte     // 标识消息的类型，* 表示在读一个数组（一个数组中有多条指令），$ 表示在读一条指令
	args              [][]byte // 已解析的传来的具体指令
	bulkLen           int64    // 传来的字节组（数据块）的长度，即 $ 符号后面跟着的数字
}

// 判断解析器是否完成

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// 上层调用 parseStream 会返回一个 channel，上层非同步阻塞、异步地从 channel 中读取指令
// ParseStream 用于并发解析指令，从 io.Reader 读取数据并通过通道（channel）发送解析后的有效载荷（Payloads）
// ParseStream 首字母大写，用作协议层对外的接口
// ParseStream 创建一个双向通道 ch，但通过返回 <-chan 将其转换为只读通道

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch) // 来一个用户开一个 parse0 协程，为每一个用户生成一个解析器
	return ch
}

// 指令解析逻辑，解析发来的指令

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for true {
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state) // 读入一行数据

		if err != nil {
			if ioErr { // 如果出现 I/O 错误，就给管道写入一个带错误信息的解析结果，并关闭管道，结束对该用户的服务
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			ch <- &Payload{ // 如果是协议错误，就给管道写入一个带错误信息的解析结果，但是不关闭管道
				Err: err,
			}
			state = readState{} // 清空当前解析器的状态，清空之前读取的数据的信息
			continue            // 继续监听用户后续的指令
		}

		// 判断是否为多行解析模式（* 开头 和 $ 开头都是多行，+OK 和 -Err 不是多行。有1个以上的\r\n换行符，就是多行。）
		if !state.readingMultiLine { // 不是多行解析模式，或者是多行但是还没初始化
			if msg[0] == '*' { // 如果解析器还没初始化，但实际上是 * 开头的多行，则这里启动多行模式
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error" + string(msg)),
					}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 { // 当出现 *0\r\n 的情况
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' { // 如果解析器还没初始化，但实际上是 $ 开头的多行，则启动多行模式
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error" + string(msg)),
					}
					state = readState{}
					continue
				}
				if state.bulkLen == -1 { // 当出现 $-1\r\n 的情况
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else { //如果解析器还没初始化，"+OK\r\n" 和 "-err\r\n" 和 ":5\r\n" 这三种单行指令，则启动单行模式
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else { // 已经初始化为了多行模式，例如"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"，之前已经读过一个*了，现在处理到 $ 了
			err := readBody(msg, &state) // 先读 $3\r\n， 下一次调用再接着读 SET\r\n，以此类推
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error" + string(msg)),
				}
				state = readState{}
				continue
			}
			if state.finished() { // 每次到该else分支调用readBody结束后，都要判断一下是否读完了整行数据
				var result resp.Reply
				// 如果读完了整行数据，判断一下这行数据是 * 开头的还是 $ 开头的，根据不同情况返回 args
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
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
	var expectedLine uint64 // 存储 * 号后的数字，如 *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n 中开头的 3, 即后面包含多少个指令
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

// readBody 用于提取 $4\r\nPING\r\n

func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2] // 去掉末尾的 \r\n
	var err error
	// 遇到的第一个字符是 $
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error" + string(msg))
		}
		// $0\r\n
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}

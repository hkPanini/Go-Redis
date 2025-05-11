package connection

import (
	"go-redis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection 用于述客户端连接
type Connection struct {
	conn         net.Conn
	waitingReply wait.Wait  // 给客户端回发数据时，如果要杀掉程序，需要等待数据回发结束
	mu           sync.Mutex // 锁，操作一个连接时，需要对其上锁
	selectedDB   int        // 指示用户正在操作哪一个 DB
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// 给客户端发送（写）数据
func (c *Connection) Write(bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}
	c.mu.Lock()           // 加锁，同一时间只能有一个协程对客户端进行写数据
	c.waitingReply.Add(1) // waitGroup + 1
	defer func() {        // 回写数据结束后 waitGroup -1，并解锁
		c.waitingReply.Done()
		c.mu.Unlock()
	}()
	_, err := c.conn.Write(bytes)
	return err
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

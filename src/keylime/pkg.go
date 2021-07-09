package keylime

import (
	"fmt"
	"net"
)

const DEFAULT_PORT = "1337"

type DialConfig struct {
	Host string
	Port string
}

func Open(cfg DialConfig) (*Conn, error) {
	port := DEFAULT_PORT

	if cfg.Port != "" {
		port = cfg.Port
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", cfg.Host, port))
	if err != nil {
		return nil, err
	}

	return &Conn{
		conn: conn,
		host: cfg.Host,
		port: port,
	}, nil
}

func Connect(host string, port string) (*Conn, error) {
	return Open(DialConfig{
		Host: host,
		Port: port,
	})
}

type Conn struct {
	conn net.Conn
	host string
	port string
}

func (c *Conn) Write(data []byte) (n int, err error) {
	c.conn.Write(data)
	res := make([]byte, 1000)
	n, err = c.conn.Read(res)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(string(res[:n]))
	}
	return n, err
}

func (c *Conn) WriteString(s string) (n int, err error) {
	return c.Write([]byte(s))
}

func (c *Conn) Close() error {
	conn := c.conn
	c.conn = nil

	return conn.Close()
}

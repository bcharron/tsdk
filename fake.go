package main

import (
	"bufio"
	"net"
	"time"
)

type FakeConn struct {
	reader   *bufio.Reader
	realConn net.Conn
}

func NewFakeConn(reader *bufio.Reader, conn net.Conn) *FakeConn {
	c := &FakeConn{
		realConn: conn,
		reader:   reader,
	}

	return (c)
}

func (c *FakeConn) Read(b []byte) (n int, err error)   { return c.reader.Read(b) }
func (c *FakeConn) Write(b []byte) (n int, err error)  { return c.realConn.Write(b) }
func (c *FakeConn) Close() error                       { return c.realConn.Close() }
func (c *FakeConn) LocalAddr() net.Addr                { return c.realConn.LocalAddr() }
func (c *FakeConn) RemoteAddr() net.Addr               { return c.realConn.RemoteAddr() }
func (c *FakeConn) SetDeadline(t time.Time) error      { return c.realConn.SetDeadline(t) }
func (c *FakeConn) SetReadDeadline(t time.Time) error  { return c.realConn.SetReadDeadline(t) }
func (c *FakeConn) SetWriteDeadline(t time.Time) error { return c.realConn.SetWriteDeadline(t) }

type FakeListener struct {
	connections chan net.Conn
	myaddr      net.Addr
}

func (l *FakeListener) Accept() (net.Conn, error) {
	// fmt.Println("Waiting for connections!")
	conn := <-l.connections
	// fmt.Println("Accept!")
	return conn, nil
}

func (l *FakeListener) Close() error { return nil }

func (l *FakeListener) Addr() net.Addr { return l.myaddr }

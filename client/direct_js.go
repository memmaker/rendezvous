//go:build js

package client

import "net/url"

// The browser has no UDP: its messages always go through the server.

type directConn struct{}

func (*directConn) send([]byte) bool       { return false }
func (*directConn) up() bool               { return false }
func (cl *Conn) startDirect(*url.URL)      {}
func (cl *Conn) connectDirect(string, int) {}
func (cl *Conn) closeDirect()              {}

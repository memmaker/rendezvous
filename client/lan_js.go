//go:build js

package client

import (
	"errors"
	"time"
)

// The browser has no sockets of its own: LAN play is for the desktop builds.

var errBrowser = errors.New("LAN play needs the desktop game")

func Announce(string, string, int)                                   {}
func Find(string, int, time.Duration) (addr, room string, err error) { return "", "", errBrowser }
func LocalIP() string                                                { return "" }

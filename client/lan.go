//go:build !js

package client

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
)

// LAN play without a server: the host's game runs a rendezvous server itself
// (rendezvous.Start) and announces its room over UDP broadcast once a second; a joiner
// on the same network listens for that and connects to the host.

// Announce tells the network about room once a second, for as long as the game runs:
// "rdv-lan NAME ROOM" to UDP port port, NAME naming the game.
func Announce(name, room string, port int) {
	c, err := net.Dial("udp4", "255.255.255.255:"+strconv.Itoa(port))
	if err != nil {
		return
	}
	go func() {
		for range time.Tick(time.Second) {
			c.Write([]byte("rdv-lan " + name + " " + room))
		}
	}()
}

// Find waits up to timeout for a host of the game name announcing itself on UDP port port
// and returns its address (ip:port) and room.
func Find(name string, port int, timeout time.Duration) (addr, room string, err error) {
	pc, err := net.ListenPacket("udp4", ":"+strconv.Itoa(port))
	if err != nil {
		return "", "", err
	}
	defer pc.Close()
	pc.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 128)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			return "", "", errors.New("No game found on this network")
		}
		if room, ok := strings.CutPrefix(string(buf[:n]), "rdv-lan "+name+" "); ok {
			host := from.(*net.UDPAddr).IP.String()
			return net.JoinHostPort(host, strconv.Itoa(port)), room, nil
		}
	}
}

// LocalIP is this machine's address on the network, for telling others where to join.
func LocalIP() string {
	c, err := net.Dial("udp4", "255.255.255.255:1")
	if err != nil {
		return "127.0.0.1"
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).IP.String()
}

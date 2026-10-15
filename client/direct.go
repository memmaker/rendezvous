//go:build !js

package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// The direct connection: the server is only the meeting point. Each player opens a UDP
// socket and sends "rdv TOKEN" to the server from it, so the server sees the player's
// public endpoint (the NAT's mapping); when both players of a room are known the server
// tells each the other's (op "peer"). Both then send to each other at once, which opens
// their NATs' holes, and the lower-numbered player accepts a QUIC connection that the
// other dials over the same socket. The game's messages go over one QUIC stream,
// length-prefixed JSON, in order and without loss; until it stands, and when the
// punching fails (symmetric NATs), they go through the server as before.

const punchFor = 8 * time.Second

type directConn struct {
	tr   *quic.Transport
	conn *quic.Conn
	s    *quic.Stream
	wmu  sync.Mutex
	busy bool // introduced already: connecting or connected
}

// startDirect opens the UDP socket and registers it with the relay at url.
func (cl *Conn) startDirect(u *url.URL) {
	pc, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		return
	}
	d := &directConn{tr: &quic.Transport{Conn: pc}}
	cl.mu.Lock()
	cl.direct = d
	cl.mu.Unlock()
	token := make([]byte, 8)
	rand.Read(token)
	b, _ := json.Marshal(request{Op: "udp", Text: hex.EncodeToString(token)})
	cl.t.Write(b)
	relay := serverUDPAddr(u)
	if relay == nil {
		return
	}
	acked := make(chan struct{})
	go func() { // what is not QUIC: the server's acknowledgement, the other's punches
		buf := make([]byte, 128)
		for {
			n, _, err := d.tr.ReadNonQUICPacket(context.Background(), buf)
			if err != nil {
				return
			}
			if string(buf[:n]) == udpPrefix+"ok" {
				select {
				case <-acked:
				default:
					close(acked)
				}
			}
		}
	}()
	go func() { // tell the server where we are until it says it knows
		for i := 0; i < 20; i++ {
			d.tr.WriteTo([]byte(udpPrefix+hex.EncodeToString(token)), relay)
			select {
			case <-acked:
				return
			case <-time.After(300 * time.Millisecond):
			}
		}
	}()
}

// udpPrefix starts the rendezvous packets (rendezvous.UDPPrefix).
const udpPrefix = "rdv "

// serverUDPAddr is the server's rendezvous address: its host and port (behind a proxy
// without one, as wss://host/path, there is none: the messages stay relayed).
func serverUDPAddr(u *url.URL) *net.UDPAddr {
	port := u.Port()
	if port == "" {
		return nil
	}
	a, err := net.ResolveUDPAddr("udp4", net.JoinHostPort(u.Hostname(), port))
	if err != nil {
		return nil
	}
	return a
}

// connectDirect punches a hole to the other player at addr and connects to it.
func (cl *Conn) connectDirect(addr string, other int) {
	cl.mu.Lock()
	d := cl.direct
	if d != nil && !d.busy {
		d.busy = true
	} else {
		d = nil
	}
	cl.mu.Unlock()
	peer, err := net.ResolveUDPAddr("udp4", addr)
	if d == nil || err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), punchFor)
	defer cancel()
	go func() { // keep knocking while the other side does the same
		for ctx.Err() == nil {
			d.tr.WriteTo([]byte(udpPrefix+"punch"), peer)
			time.Sleep(200 * time.Millisecond)
		}
	}()
	cfg := &quic.Config{KeepAlivePeriod: 5 * time.Second, MaxIdleTimeout: 30 * time.Second}
	if cl.Me < other {
		l, err := d.tr.Listen(serverTLS(), cfg)
		if err != nil {
			return
		}
		defer l.Close()
		if d.conn, err = l.Accept(ctx); err != nil {
			return
		}
		if d.s, err = d.conn.AcceptStream(ctx); err != nil {
			return
		}
	} else {
		if d.conn, err = d.tr.Dial(ctx, peer, clientTLS(), cfg); err != nil {
			return
		}
		if d.s, err = d.conn.OpenStreamSync(ctx); err != nil {
			return
		}
		d.send([]byte(`{"op":"direct"}`)) // opens the stream on the other side
	}
	log.Printf("rendezvous: direct connection to player %d (%s)", other, d.conn.RemoteAddr())
	cl.mu.Lock()
	cl.direct = d
	cl.mu.Unlock()
	go cl.readDirect(d, other)
}

func (d *directConn) up() bool {
	d.wmu.Lock()
	defer d.wmu.Unlock()
	return d.s != nil
}

// send writes the message b to the other player if the connection stands.
func (d *directConn) send(b []byte) bool {
	d.wmu.Lock()
	defer d.wmu.Unlock()
	if d.s == nil {
		return false
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(b)))
	d.s.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := d.s.Write(n[:]); err != nil {
		return false
	}
	_, err := d.s.Write(b)
	return err == nil
}

// readDirect takes the other player's messages until the connection goes; the server
// takes over then.
func (cl *Conn) readDirect(d *directConn, other int) {
	var n [4]byte
	for {
		if _, err := io.ReadFull(d.s, n[:]); err != nil {
			break
		}
		size := binary.BigEndian.Uint32(n[:])
		if size > ReadLimit {
			break
		}
		b := make([]byte, size)
		if _, err := io.ReadFull(d.s, b); err != nil {
			break
		}
		var m struct{ Op string }
		if json.Unmarshal(b, &m) != nil || m.Op == "direct" {
			continue
		}
		cl.handle(Message{From: other, Data: b})
	}
	log.Printf("rendezvous: direct connection lost, relaying")
	d.wmu.Lock()
	d.s = nil
	d.wmu.Unlock()
}

func (cl *Conn) closeDirect() {
	cl.mu.Lock()
	d := cl.direct
	cl.direct = nil
	cl.mu.Unlock()
	if d != nil {
		if d.conn != nil {
			d.conn.CloseWithError(0, "bye")
		}
		d.tr.Close()
	}
}

// ponytail: the players were introduced by the server, so the TLS QUIC insists on is a
// throwaway self-signed certificate the other side does not check. Pin its hash through
// the server if someone on the path is a worry.
func serverTLS() *tls.Config {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour)}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, NextProtos: []string{"uw1go"}}
}

func clientTLS() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"uw1go"}}
}

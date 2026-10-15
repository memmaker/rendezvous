// Package rendezvous is a meeting point for peer-to-peer games: it keeps rooms per game,
// tells the players of a room each other's public UDP endpoint so they can connect to
// each other directly (hole punching), and passes their messages on while they cannot.
// It knows nothing of any game: messages are JSON objects, forwarded as they are.
//
// Players connect over WebSocket or plain TCP with one JSON object per line, both on
// the same port, and send a UDP packet to the same port for the rendezvous. See the
// README for the protocol.
package rendezvous

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// ReadLimit bounds one message.
const ReadLimit = 1 << 20

// UDPPrefix starts the players' rendezvous packets: "rdv TOKEN", answered with "rdv ok".
const UDPPrefix = "rdv "

type peer struct {
	num   int
	game  string
	room  string
	out   chan []byte
	token string   // the peer's UDP registration (op "udp")
	udp   net.Addr // where its UDP packets come from: its public endpoint
}

// Server keeps the rooms, per game.
type Server struct {
	// Players is how many a room holds (default 2).
	Players int
	mu      sync.Mutex
	rooms   map[string]map[string]map[int]*peer // game, room, player number
	peers   map[string]*peer                    // by UDP token
	conns   int
	idle    time.Time // when the last connection went
}

func New() *Server {
	return &Server{Players: 2, rooms: map[string]map[string]map[int]*peer{}, peers: map[string]*peer{}, idle: time.Now()}
}

// Serve runs a server on addr (such as ":7781"), TCP for the players and UDP for the
// rendezvous, or on the sockets systemd passes (LISTEN_FDS: the stream, then the
// datagram socket). With idle above zero it returns once no one has been connected for
// that long; systemd starts it again on the next connection.
func Serve(addr string, idle time.Duration) error {
	s, l, pc, err := listen(addr)
	if err != nil {
		return err
	}
	if idle > 0 {
		go func() {
			for range time.Tick(time.Minute) {
				s.mu.Lock()
				done := s.conns == 0 && time.Since(s.idle) > idle
				s.mu.Unlock()
				if done {
					log.Print("idle, stopping")
					l.Close()
					pc.Close()
					return
				}
			}
		}()
	}
	log.Printf("rendezvous on %s", l.Addr())
	if err := s.serve(l, pc); err != nil && idle > 0 {
		return nil // closed for idleness
	} else {
		return err
	}
}

// Start runs a server on addr in the background (a game hosting on a LAN) and returns it
// with the address it listens on.
func Start(addr string) (*Server, net.Addr, error) {
	s, l, pc, err := listen(addr)
	if err != nil {
		return nil, nil, err
	}
	go s.serve(l, pc)
	return s, l.Addr(), nil
}

func listen(addr string) (*Server, net.Listener, net.PacketConn, error) {
	var l net.Listener
	var pc net.PacketConn
	var err error
	if fds := os.Getenv("LISTEN_FDS"); fds != "" {
		l, err = net.FileListener(os.NewFile(3, "systemd"))
		if err == nil && fds == "2" {
			pc, err = net.FilePacketConn(os.NewFile(4, "systemd udp"))
		}
	} else {
		l, err = net.Listen("tcp", addr)
		if err == nil {
			pc, err = net.ListenPacket("udp", l.Addr().String()) // the same port, also with addr ":0"
		}
	}
	if err != nil {
		return nil, nil, nil, err
	}
	return New(), l, pc, nil
}

// serve takes the players on l, WebSocket or JSON lines by what they send first, and
// the rendezvous packets on pc.
func (s *Server) serve(l net.Listener, pc net.PacketConn) error {
	if pc != nil {
		go s.ServeUDP(pc)
	}
	web := &chanListener{ch: make(chan net.Conn), addr: l.Addr(), done: make(chan struct{})}
	go http.Serve(web, s)
	defer web.Close()
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go func() {
			c.SetReadDeadline(time.Now().Add(30 * time.Second))
			br := bufio.NewReaderSize(c, 4096)
			head, _ := br.Peek(4)
			c.SetReadDeadline(time.Time{})
			pc := &peekedConn{Conn: c, r: br}
			if string(head) == "GET " {
				web.ch <- pc
			} else {
				s.session(&lineConn{r: br, c: pc})
			}
		}()
	}
}

// ServeHTTP takes a player over WebSocket.
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	c, err := websocket.Accept(w, req, &websocket.AcceptOptions{InsecureSkipVerify: true}) // any page may play
	if err != nil {
		return
	}
	c.SetReadLimit(ReadLimit)
	s.session(&wsConn{c: c, ctx: req.Context()})
}

// conn is one player's connection, whole messages in and out.
type conn interface {
	Read() ([]byte, error)
	Write([]byte) error
	Close()
}

type wsConn struct {
	c   *websocket.Conn
	ctx context.Context
}

func (w *wsConn) Read() ([]byte, error) { _, b, err := w.c.Read(w.ctx); return b, err }
func (w *wsConn) Write(b []byte) error  { return w.c.Write(w.ctx, websocket.MessageText, b) }
func (w *wsConn) Close()                { w.c.CloseNow() }

type lineConn struct {
	r *bufio.Reader
	c net.Conn
}

func (l *lineConn) Read() ([]byte, error) {
	var b []byte
	for {
		part, isPrefix, err := l.readLine()
		if err != nil {
			return nil, err
		}
		b = append(b, part...)
		if len(b) > ReadLimit {
			return nil, errors.New("message too long")
		}
		if !isPrefix {
			return b, nil
		}
	}
}

func (l *lineConn) readLine() ([]byte, bool, error) {
	part, err := l.r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return part, true, nil
	}
	return part, false, err
}

func (l *lineConn) Write(b []byte) error {
	l.c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := l.c.Write(append(b, '\n'))
	return err
}
func (l *lineConn) Close() { l.c.Close() }

// session runs one player: its requests to the server, and its messages to its room.
func (s *Server) session(c conn) {
	p := &peer{out: make(chan []byte, 512)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for m := range p.out {
			if c.Write(m) != nil {
				c.Close()
				return
			}
		}
	}()
	s.mu.Lock()
	s.conns++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.conns--
		s.idle = time.Now()
		delete(s.peers, p.token)
		if p.room != "" {
			ps := s.rooms[p.game][p.room]
			delete(ps, p.num)
			if len(ps) == 0 {
				delete(s.rooms[p.game], p.room)
				if len(s.rooms[p.game]) == 0 {
					delete(s.rooms, p.game)
				}
			}
			s.broadcast(ps, p, encode(reply{Op: "left", From: p.num}))
		}
		s.mu.Unlock()
		close(p.out)
		<-done
		c.Close()
	}()
	for {
		raw, err := c.Read()
		if err != nil {
			return
		}
		raw = bytes.TrimSpace(raw)
		var m struct{ Op, Game, Room, Text string }
		if json.Unmarshal(raw, &m) != nil || len(raw) < 2 || raw[len(raw)-1] != '}' {
			continue
		}
		s.mu.Lock()
		switch m.Op {
		case "host":
			if p.room == "" {
				rooms := s.rooms[m.Game]
				if rooms == nil {
					rooms = map[string]map[int]*peer{}
					s.rooms[m.Game] = rooms
				}
				name := RoomName()
				for rooms[name] != nil {
					name = RoomName()
				}
				p.game, p.room, p.num = m.Game, name, 0
				rooms[name] = map[int]*peer{0: p}
			}
			s.reply(p, reply{Op: "room", Room: p.room, From: p.num})
		case "list":
			names := []string{}
			for name, ps := range s.rooms[m.Game] {
				if len(ps) < s.Players {
					names = append(names, name)
				}
			}
			sort.Strings(names)
			s.reply(p, reply{Op: "rooms", Rooms: names})
		case "join":
			ps := s.rooms[m.Game][strings.ToUpper(m.Room)]
			switch {
			case p.room != "" || ps == nil:
				s.reply(p, reply{Op: "error", Err: "No such game"})
			case len(ps) >= s.Players:
				s.reply(p, reply{Op: "error", Err: "That game is full"})
			default:
				for ps[p.num] != nil {
					p.num++
				}
				p.game, p.room = m.Game, strings.ToUpper(m.Room)
				ps[p.num] = p
				s.reply(p, reply{Op: "joined", Room: p.room, From: p.num})
				s.introduce(p)
			}
		case "udp": // the peer will send "rdv TOKEN" over UDP; that tells us its endpoint
			delete(s.peers, p.token)
			p.token = m.Text
			s.peers[p.token] = p
		default: // the game's: passed on as it is, with the sender's number (the last "from" counts)
			if p.room != "" {
				from, _ := json.Marshal(p.num)
				fwd := append(append(append(raw[:len(raw)-1:len(raw)-1], `,"from":`...), from...), '}')
				s.broadcast(s.rooms[p.game][p.room], p, fwd)
			}
		}
		s.mu.Unlock()
	}
}

// ServeUDP takes the peers' rendezvous packets on pc: "rdv TOKEN" tells where the peer
// with that token is reachable, and is answered with "rdv ok".
func (s *Server) ServeUDP(pc net.PacketConn) {
	buf := make([]byte, 128)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			return
		}
		token, ok := bytes.CutPrefix(buf[:n], []byte(UDPPrefix))
		if !ok {
			continue
		}
		s.mu.Lock()
		if p := s.peers[string(token)]; p != nil {
			p.udp = from
			s.introduce(p)
		}
		s.mu.Unlock()
		pc.WriteTo([]byte(UDPPrefix+"ok"), from)
	}
}

// introduce tells p and each other player of its room where the other is reachable over
// UDP (op "peer"), once both are known; they then try to connect to each other directly.
func (s *Server) introduce(p *peer) {
	if p.udp == nil {
		return
	}
	for _, q := range s.rooms[p.game][p.room] {
		if q != p && q.udp != nil {
			s.reply(p, reply{Op: "peer", From: q.num, Text: q.udp.String()})
			s.reply(q, reply{Op: "peer", From: p.num, Text: p.udp.String()})
		}
	}
}

// reply is what the server itself says.
type reply struct {
	Op    string   `json:"op"`
	Room  string   `json:"room,omitempty"`
	Rooms []string `json:"rooms,omitempty"`
	From  int      `json:"from"`
	Err   string   `json:"err,omitempty"`
	Text  string   `json:"text,omitempty"`
}

func encode(m reply) []byte {
	b, _ := json.Marshal(m)
	return b
}

// reply and broadcast drop messages for a peer too slow to take them.
func (s *Server) reply(p *peer, m reply) { s.send(p, encode(m)) }

func (s *Server) send(p *peer, m []byte) {
	select {
	case p.out <- m:
	default:
	}
}

func (s *Server) broadcast(ps map[int]*peer, from *peer, m []byte) {
	for _, q := range ps {
		if q != from {
			s.send(q, m)
		}
	}
}

// RoomName makes a room name of four letters easy to read out.
func RoomName() string {
	const letters = "ABCDEFGHJKLMNPQRSTUVWXYZ"
	b := make([]byte, 4)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

// chanListener hands the HTTP server the connections that turned out to be WebSocket.
type chanListener struct {
	ch   chan net.Conn
	addr net.Addr
	done chan struct{}
	once sync.Once
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *chanListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *chanListener) Addr() net.Addr { return l.addr }

// peekedConn reads what was peeked at first.
type peekedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *peekedConn) Read(b []byte) (int, error) { return c.r.Read(b) }

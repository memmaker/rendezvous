// Package client connects a game to a rendezvous server: it hosts or joins a room, sends
// the game's messages (JSON objects) to the other player and takes theirs. Once the
// server has introduced the two and the hole punching worked, the messages go directly
// between the players over QUIC (direct.go); until then, and where that fails, through
// the server. In the browser (GOOS=js) they always go through the server.
package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// ReadLimit bounds one message.
const ReadLimit = 1 << 20

// Message is one from the other player: who sent it and the JSON object as it was. A
// player who left is reported as {"op":"left"} from their number.
type Message struct {
	From int
	Data json.RawMessage
}

// Conn is one player's connection.
type Conn struct {
	game   string
	t      transport
	answer chan control
	out    chan []byte
	direct *directConn
	mu     sync.Mutex
	events []Message
	closed bool
	Room   string
	Me     int // this player's number in the room
}

// control is what the server itself says.
type control struct {
	Op    string   `json:"op"`
	Room  string   `json:"room,omitempty"`
	Rooms []string `json:"rooms,omitempty"`
	From  int      `json:"from"`
	Err   string   `json:"err,omitempty"`
	Text  string   `json:"text,omitempty"`
}

// Dial connects to the server at rawurl (ws://, wss:// or tcp://host:port; a bare
// host:port is tcp) for the game named game: its rooms are its own. Games should put
// their protocol version into the name so that builds that cannot play together never
// meet.
func Dial(rawurl, game string) (*Conn, error) {
	if !strings.Contains(rawurl, "://") {
		rawurl = "tcp://" + rawurl
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var t transport
	switch u.Scheme {
	case "ws", "wss":
		c, _, err := websocket.Dial(ctx, rawurl, nil)
		if err != nil {
			return nil, err
		}
		c.SetReadLimit(ReadLimit)
		t = &wsTransport{c: c}
	default:
		t, err = dialTCP(ctx, u.Host)
		if err != nil {
			return nil, err
		}
	}
	cl := &Conn{game: game, t: t, answer: make(chan control, 1), out: make(chan []byte, 256)}
	go cl.read()
	go cl.write()
	cl.startDirect(u)
	return cl, nil
}

type transport interface {
	Read() ([]byte, error)
	Write([]byte) error
	Close()
}

type wsTransport struct{ c *websocket.Conn }

func (w *wsTransport) Read() ([]byte, error) {
	_, b, err := w.c.Read(context.Background())
	return b, err
}
func (w *wsTransport) Write(b []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return w.c.Write(ctx, websocket.MessageText, b)
}
func (w *wsTransport) Close() { w.c.Close(websocket.StatusNormalClosure, "") }

type tcpTransport struct {
	c net.Conn
	r *bufio.Reader
}

func dialTCP(ctx context.Context, addr string) (*tcpTransport, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &tcpTransport{c: c, r: bufio.NewReaderSize(c, 64<<10)}, nil
}

func (t *tcpTransport) Read() ([]byte, error) {
	var b []byte
	for {
		part, err := t.r.ReadSlice('\n')
		b = append(b, part...)
		if err == bufio.ErrBufferFull {
			if len(b) > ReadLimit {
				return nil, errors.New("message too long")
			}
			continue
		}
		return b, err
	}
}
func (t *tcpTransport) Write(b []byte) error {
	t.c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := t.c.Write(append(b, '\n'))
	return err
}
func (t *tcpTransport) Close() { t.c.Close() }

func (cl *Conn) read() {
	defer close(cl.answer)
	for {
		b, err := cl.t.Read()
		if err != nil {
			return
		}
		var m control
		if json.Unmarshal(b, &m) != nil {
			continue
		}
		switch m.Op {
		case "room", "rooms", "joined", "error":
			select {
			case cl.answer <- m:
			default:
			}
		case "peer":
			go cl.connectDirect(m.Text, m.From)
		default:
			cl.handle(Message{From: m.From, Data: b})
		}
	}
}

// handle keeps a message from the other player for Recv.
func (cl *Conn) handle(m Message) {
	cl.mu.Lock()
	cl.events = append(cl.events, m)
	cl.mu.Unlock()
}

// write sends what is queued: the game's messages to the other player directly where that
// connection stands, else through the server.
func (cl *Conn) write() {
	for b := range cl.out {
		cl.mu.Lock()
		d := cl.direct
		cl.mu.Unlock()
		if d != nil && d.send(b) {
			continue
		}
		cl.t.Write(b)
	}
}

func (cl *Conn) ask(m any) (control, error) {
	b, _ := json.Marshal(m)
	cl.t.Write(b) // the server's own requests: never direct
	r, ok := <-cl.answer
	if !ok {
		return control{}, errors.New("the server closed the connection")
	}
	if r.Err != "" {
		return r, errors.New(r.Err)
	}
	return r, nil
}

type request struct {
	Op   string `json:"op"`
	Game string `json:"game,omitempty"`
	Room string `json:"room,omitempty"`
	Text string `json:"text,omitempty"`
}

// Host opens a room and returns its name.
func (cl *Conn) Host() (string, error) {
	r, err := cl.ask(request{Op: "host", Game: cl.game})
	cl.Room, cl.Me = r.Room, r.From
	return r.Room, err
}

// List gives the game's open rooms.
func (cl *Conn) List() ([]string, error) {
	r, err := cl.ask(request{Op: "list", Game: cl.game})
	return r.Rooms, err
}

// Join enters a room.
func (cl *Conn) Join(room string) error {
	r, err := cl.ask(request{Op: "join", Game: cl.game, Room: room})
	if err == nil {
		cl.Room, cl.Me = r.Room, r.From
	}
	return err
}

// Send passes v, a JSON object, to the other players, in order, without waiting for the
// network. With the network that far behind it is dropped. Its "op" must not be one of
// the server's (host, list, join, udp).
func (cl *Conn) Send(v any) {
	b, err := json.Marshal(v)
	if err != nil || len(b) < 2 || b[0] != '{' {
		return
	}
	select {
	case cl.out <- b:
	default:
	}
}

// Recv takes the messages that came in since the last call.
func (cl *Conn) Recv() []Message {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	ev := cl.events
	cl.events = nil
	return ev
}

// Direct tells whether the game's messages go straight to the other player.
func (cl *Conn) Direct() bool {
	cl.mu.Lock()
	d := cl.direct
	cl.mu.Unlock()
	return d != nil && d.up()
}

// Close leaves the server and the other player.
func (cl *Conn) Close() {
	cl.mu.Lock()
	closed := cl.closed
	cl.closed = true
	cl.mu.Unlock()
	if closed {
		return
	}
	cl.t.Close()
	cl.closeDirect()
}

package rendezvous_test

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/memmaker/rendezvous"
	"github.com/memmaker/rendezvous/client"
)

// wait polls f for up to three seconds.
func wait(t *testing.T, what string, f func() bool) {
	t.Helper()
	for i := 0; !f(); i++ {
		if i > 300 {
			t.Fatal(what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func start(t *testing.T) string {
	_, addr, err := rendezvous.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return addr.String()
}

func dial(t *testing.T, url, game string) *client.Conn {
	c, err := client.Dial(url, game)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

type hello struct {
	Op   string `json:"op"`
	Text string `json:"text"`
}

func got(t *testing.T, c *client.Conn, from int, text string) {
	t.Helper()
	wait(t, "no "+text, func() bool {
		for _, m := range c.Recv() {
			var h hello
			json.Unmarshal(m.Data, &h)
			if h.Op == "hello" && h.Text == text && m.From == from {
				return true
			}
		}
		return false
	})
}

// A WebSocket host and a plain TCP joiner meet on one port, their messages cross, rooms
// are per game, and (on a real network) they connect to each other directly.
func TestServer(t *testing.T) {
	addr := start(t)
	h := dial(t, "ws://"+addr, "game-1")
	room, err := h.Host()
	if err != nil {
		t.Fatal(err)
	}
	other := dial(t, addr, "game-2")
	if rooms, _ := other.List(); len(rooms) != 0 {
		t.Fatalf("another game sees the room: %v", rooms)
	}
	if err := other.Join(room); err == nil {
		t.Fatal("another game joined the room")
	}
	j := dial(t, addr, "game-1")
	if rooms, _ := j.List(); len(rooms) != 1 || rooms[0] != room {
		t.Fatalf("rooms: %v, want [%s]", rooms, room)
	}
	if err := j.Join(room); err != nil {
		t.Fatal(err)
	}
	if third := dial(t, addr, "game-1"); third.Join(room) == nil {
		t.Fatal("a third player got in")
	}
	j.Send(hello{"hello", "from joiner"})
	h.Send(hello{"hello", "from host"})
	got(t, h, 1, "from joiner")
	got(t, j, 0, "from host")
	if _, err := net.ListenPacket("udp4", ":0"); err == nil {
		wait(t, "no direct connection", func() bool { return h.Direct() && j.Direct() })
		j.Send(hello{"hello", "direct"})
		got(t, h, 1, "direct")
	}
	j.Close()
	wait(t, "no left", func() bool {
		for _, m := range h.Recv() {
			var c struct{ Op string }
			json.Unmarshal(m.Data, &c)
			if c.Op == "left" && m.From == 1 {
				return true
			}
		}
		return false
	})
}

// Half a message, junk and an overlong line do not take the server down.
func TestJunk(t *testing.T) {
	addr := start(t)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.Write([]byte("not json\n{\"op\":\"host\"\n"))
	c.Close()
	if _, err := dial(t, addr, "g").Host(); err != nil {
		t.Fatal(err)
	}
}

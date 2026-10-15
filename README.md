# rendezvous

A meeting point for peer-to-peer games, shared by any number of them. It keeps rooms
per game, tells the two players of a room each other's public UDP endpoint so they can
connect to each other directly (UDP hole punching), and passes their messages on while
they cannot. It knows nothing of any game: messages are JSON objects, forwarded as they
are. One instance serves Ultima Underworld (uw1go), OXCE Battles and Phoenix Battles.

```sh
go run ./cmd/rendezvous -addr :7781      # TCP and UDP on 7781 must be open
```

Releases carry binaries for Linux, Windows and macOS. `deploy/install.sh` installs it on
a server as a systemd socket-activated service that exits when idle.

## Protocol

Players connect over **WebSocket** (`ws://host:7781`) or **plain TCP with one JSON
object per line** (`host:7781`), both on the same port, and speak JSON objects with an
`op`. Requests to the server, each answered once:

| request | answer |
|---|---|
| `{"op":"host","game":G}` | `{"op":"room","room":"ABCD","from":0}` — a new room, this player is number 0 |
| `{"op":"list","game":G}` | `{"op":"rooms","rooms":["ABCD",...]}` — the game's rooms with a free seat |
| `{"op":"join","game":G,"room":"ABCD"}` | `{"op":"joined","room":"ABCD","from":1}` or `{"op":"error","err":"No such game"}` |
| `{"op":"udp","text":TOKEN}` then the UDP packet `rdv TOKEN` to the same port | the UDP packet `rdv ok` |

`game` names the game and should carry its protocol version (`uw1go-online-5`), so that
builds that cannot play together never meet. Rooms hold two players and go when the last
one leaves.

Any other object is **forwarded** to the other players of the room with `"from":N`
added (the sender's number). The server itself sends `{"op":"left","from":N}` when a
player goes, and, once it knows both players' UDP endpoints,
`{"op":"peer","from":N,"text":"ip:port"}` to each: the other's public address. Both
then send UDP packets to each other, which opens their NATs, and connect over the same
sockets; what runs over the hole is the players' business. The Go client (`client`)
uses QUIC: the lower-numbered player accepts, the other dials, and the game's messages go
over one stream as length-prefixed JSON (4 bytes big-endian, then the object). Where the
punching fails (both behind symmetric NATs) the messages keep going through the server.

## Go client

```go
c, err := client.Dial("ruzzoli.de:7781", "mygame-1")   // host:port (TCP), ws:// or wss://
room, err := c.Host()                                   // or c.List(), c.Join(room)
c.Send(map[string]any{"op": "move", "x": 3})            // any JSON object
for _, m := range c.Recv() { ... m.From, m.Data ... }   // the other's messages, in order
c.Direct()                                              // true once the players connect directly
```

Without a server, a host runs one itself: `rendezvous.Start(":7781")`, then
`client.Announce(game, room, 7781)` tells the LAN by UDP broadcast and a joiner's
`client.Find(game, 7781, timeout)` hears it. In the browser (GOOS=js) the client speaks
WebSocket and always goes through the server.

// Command rendezvous is a meeting point for peer-to-peer games: rooms per game, the
// players' public UDP endpoints for hole punching, and a relay for their messages while
// they have no direct connection. Run it anywhere: `rendezvous -addr :7781`, with TCP and
// UDP on that port open. Under systemd it takes the sockets it is given (deploy/) and
// stops after a while with no one connected.
package main

import (
	"flag"
	"log"

	"github.com/memmaker/rendezvous"
)

func main() {
	addr := flag.String("addr", ":7781", "where to listen (TCP and UDP) without sockets from systemd")
	idle := flag.Duration("idle", 0, "stop after this long with no one connected (0: never)")
	flag.Parse()
	if err := rendezvous.Serve(*addr, *idle); err != nil {
		log.Fatal(err)
	}
}

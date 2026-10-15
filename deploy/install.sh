#!/bin/sh
# install.sh: build the server for Linux and install it on $HOST (default ruzzoli.de) as a
# systemd socket service on TCP+UDP 7781 (once, and again after changes). Asks for the
# sudo password on the server.
set -e
cd "$(dirname "$0")/.."
host=${HOST:-ruzzoli.de}
B=$(mktemp -d)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags=-s -o "$B/rendezvous" ./cmd/rendezvous
cp deploy/rendezvous.socket deploy/rendezvous.service "$B/"
ssh "$host" 'mkdir -p ~/rendezvous'
scp -q "$B"/* "$host:rendezvous/"
rm -rf "$B"
ssh -t "$host" 'set -e
	sudo install -m 755 ~/rendezvous/rendezvous /usr/local/bin/rendezvous
	sudo install -m 644 ~/rendezvous/rendezvous.socket ~/rendezvous/rendezvous.service /etc/systemd/system/
	sudo systemctl daemon-reload
	sudo systemctl stop rendezvous.service 2>/dev/null || true
	sudo systemctl enable --now rendezvous.socket
	if command -v ufw >/dev/null; then sudo ufw allow 7781/tcp && sudo ufw allow 7781/udp; fi
	systemctl status --no-pager rendezvous.socket | head -5'

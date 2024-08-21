package rft

import (
	"errors"
	"io"
	"log/slog"
	"net"
)

type RaftNet struct {
	nodeName string
	address  string
	listener net.Listener
}

func NewRaftNet(nodeName string) *RaftNet {
	address := SERVERS[nodeName]
	listener, err := net.Listen("tcp", address)
	if err != nil {
		slog.Error("error listening on TCP:", "error", err)
		panic(err)
	}

	return &RaftNet{
		nodeName: nodeName,
		address:  address,
		listener: listener,
	}
}

func (rn *RaftNet) Send(destination string, message []byte) {
	destAddr := SERVERS[destination]
	conn, err := net.Dial("tcp", destAddr)
	if err != nil {
		slog.Error("error dialing TCP:", "error", err)
		// panic(err)
		return
	}
	defer conn.Close()

	if err := Send(conn, message); err != nil {
		slog.Error("error sending message:", "error", err)
	}
}

func (rn *RaftNet) Receive() []byte {
	conn, err := rn.listener.Accept()
	if err != nil {
		slog.Error("error accepting connection:", "error", err)
		return nil
	}
	defer conn.Close()

	msg, err := Rcv_msg(conn)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			slog.Error("error receiving message: %w", "error", err)
			return nil
		}
		panic(err)
	}

	return msg
}

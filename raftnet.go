package rft

import (
	"errors"
	"fmt"
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

func (rn *RaftNet) Send(addr string, msg []byte) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		slog.Error("Error connecting to server", "error", err)
		return err
	}
	defer conn.Close()

	err = Send(conn, msg)
	if err != nil {
		slog.Error("Error sending message", "error", err)
		return err
	}

	return nil
}

func (rn *RaftNet) SendInConnection(conn net.Conn, msg []byte) error {
	err := Send(conn, msg)
	if err != nil {
		slog.Error("Error sending message", "error", err)
		return err
	}
	return nil
}

func (rn *RaftNet) Receive(conn net.Conn) ([]byte, error) {
	conn, err := rn.listener.Accept()
	if err != nil {
		slog.Error("error accepting connection:", "error", err)
		return nil, err
	}
	defer conn.Close()

	msg, err := Rcv_msg(conn)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			slog.Error("error receiving message: %w", "error", err)
			return nil, err
		}
		panic(err)
	}

	return msg, nil
}

func (rn *RaftNet) ReceiveFromConnection(conn net.Conn) ([]byte, error) {
	msgBytes, err := Rcv_msg(conn)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			slog.Error("Error receiving message", "error", err)
		}
		return nil, fmt.Errorf("error receiving message: %w", err)
	}

	return msgBytes, nil
}

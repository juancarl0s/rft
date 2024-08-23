package rft

// import (
// 	"encoding/json"
// 	"fmt"
// 	"log/slog"
// 	"net"
// )

// type Raft struct {
// 	RaftLogic *RaftLogic
// 	RaftNet   *RaftNet
// }

// func (rf *Raft) ListenForMessages(listener net.Listener, msgHandler func(msg Message)) {
// 	for {
// 		// Accept a new connection
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			slog.Error("Error accepting connection:", "error", err)
// 			continue
// 		}

// 		go func(conn net.Conn) {
// 			defer conn.Close()

// 			msgBytes, err := rf.RaftNet.ReceiveFromConnection(conn)
// 			if err != nil {
// 				return
// 			}

// 			var msg Message
// 			err = json.Unmarshal(msgBytes, &msg)
// 			if err != nil {
// 				slog.Error("Error decoding JSON:", "error", err)
// 				return
// 			}

// 			rf.MessageHandler(msg)
// 		}(conn)
// 	}
// }

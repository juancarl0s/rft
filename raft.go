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

// func (rf *Raft) MessageHandler(msg Message) {
// 	if msg.MsgType == SUBMIT_COMMAND_MSG && msg.SubmitCommandRequest != nil && *msg.SubmitCommandRequest == "log" {
// 		fmt.Printf("\n\nLog.Entries:\n%+v\n\n", rf.Log.Entries)
// 		fmt.Printf("RaftServer:\n%+v\n\n", rf)
// 		fmt.Printf("KVStore:\n%+v\n\n", rf.stateMachineCommandHandler.String())
// 		if rf.RaftLogic.Role == "leader" {
// 			rf.RaftLogic.ForwardLogCommandToFollowers()
// 		}
// 		return
// 	}

// 	if rf.RaftLogic.Role == "leader" {
// 		if msg.MsgType == SUBMIT_COMMAND_MSG && msg.SubmitCommandRequest != nil {
// 			_, err := rf.RaftLogic.stateMachineCommandHandler.HandleCommand(*msg.SubmitCommandRequest)
// 			if err != nil {
// 				slog.Error("Error running state machine command", "error", err)
// 				Send(conn, []byte("BAD"))
// 				return
// 			}
// 			rf.RaftLogic.SubmitNewCommand(*msg.SubmitCommandRequest)
// 			Send(conn, []byte("OK"))
// 			// rf.SendAppendEntriesToAllFollowers()

// 		} else if msg.MsgType == APPEND_ENTRIES_RESPONSE_MSG {
// 			rf.RaftLogic.handleAppendEntriesResponse(*msg.AppendEntriesResponse)
// 		} else {
// 			slog.Error("Invalid message type", "msgType", msg.MsgType, "msg", msg)
// 		}

// 	} else if rf.RaftLogic.Role == "follower" {
// 		if msg.MsgType == APPEND_ENTRIES_MSG {
// 			// fmt.Printf("\nReceived message:\n%+v\n\n", msg.AppendEntriesRequest)
// 			appendEntriesResult := rf.RaftLogic.handleAppendEntriesRequest(msg)
// 			rf.RaftLogic.SendAppendEntriesResponse(appendEntriesResult)
// 		}
// 	}
// }

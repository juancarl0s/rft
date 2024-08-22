package rft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
)

type RaftLogic struct {
	// Cluster config details
	Nodename    string
	clusterSize int

	Role string

	// Persistent state on all servers
	currentTerm int // initialized to 0 on first boot, increases monotonically
	// votedFor string
	Log *Log

	// Volatile state on all servers
	commitIdx      int // initialized to 0, increases monotonically
	lastAppliedIdx int // index of highest log entry applied to state machine, initialized to 0, increases monotonically

	// Volatile state on leaders
	nextIdxs  map[string]int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	macthIdxs map[string]int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

func NewRaftLogic(nodename string, role string, term int) *RaftLogic {
	nextIdx := map[string]int{}
	matchIds := map[string]int{}
	for name := range SERVERS {
		if name == nodename {
			continue
		}
		nextIdx[name] = 0
		matchIds[name] = 0
	}

	rf := &RaftLogic{
		Nodename:    nodename,
		clusterSize: len(SERVERS),

		Role: role,

		currentTerm: term,
		Log:         NewLog(),

		commitIdx:      0,
		lastAppliedIdx: 0,

		nextIdxs:  map[string]int{},
		macthIdxs: map[string]int{},
	}

	rf.Log.AppendCommand(0, "initial dummy command") // maybe not?

	// Populate nextIdx and matchIds for each server
	for name := range SERVERS {
		if name == nodename {
			continue
		}
		rf.nextIdxs[name] = 1
		rf.macthIdxs[name] = 0
	}

	return rf
}

func (rf *RaftLogic) BecomeLeader() {
	rf.Role = "leader"
}

// AppenEntries to local log
func (rf *RaftLogic) SubmitNewCommand(cmd string) {
	if rf.Role != "leader" {
		panic("Only leader can send AppendEntries to followers")
	}
	rf.Log.AppendCommand(rf.currentTerm, cmd)
}

// func (rf *RaftLogic) ReceiveAppendEntries(appendEntries AppendEntriesRequest) {
// 	if appendEntries.LeaderTerm < rf.currentTerm {
// 		// TODO: Reply false
// 		return
// 	} else if appendEntries.LeaderTerm > rf.currentTerm {
// 		rf.currentTerm = appendEntries.LeaderTerm
// 	}

// 	// Append entries to log
// 	rf.Log.AppendEntries(appendEntries)
// }

// func (rf *RaftLogic) Receive() {
// 	addr, exists := SERVERS[rf.Nodename]
// 	if !exists {
// 		panic(fmt.Sprintf("Node '%s' not found in SERVERS", rf.Nodename))
// 	}

// 	listener, err := net.Listen("tcp", addr)
// 	if err != nil {
// 		slog.Error("Error binding to port", "error", err)
// 		panic(err)
// 	}
// 	defer listener.Close()

// 	for {
// 		// Accept a new connection
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			slog.Error("Error accepting connection:", "error", err)
// 			continue
// 		}
// 		addr := conn.RemoteAddr().String()
// 		slog.Debug("Connection from", "addr", addr)

// 		go rf.HandleIncomingMsg(conn)
// 	}
// }

func (rf *RaftLogic) ReceiveV2(listener net.Listener) {
	for {
		// Accept a new connection
		conn, err := listener.Accept()
		if err != nil {
			slog.Error("Error accepting connection:", "error", err)
			continue
		}
		addr := conn.RemoteAddr().String()
		slog.Debug("Connection from", "addr", addr)

		go rf.HandleIncomingMsg(conn)
	}
}

func (rf *RaftLogic) HandleIncomingMsg(conn net.Conn) {
	fmt.Println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
	defer conn.Close()

	for {
		msgBytes, err := Rcv_msg(conn)
		if err != nil {
			// slog.Error("Error receiving message:", "error", err, "msgBytes", msgBytes)

			// if errors.Is(err, io.EOF) {
			// 	slog.Error("@@@@@@@@@@@@@@@@@@", "error", err, "msgBytes", msgBytes)
			// 	return
			// }

			// continue
			// // return

			if !errors.Is(err, io.EOF) {
				err = fmt.Errorf("error receiving message: %w", err)
				slog.Error("Error sending back response:", "error", err)
				return
			}
			return
		}

		var msg Message
		err = json.Unmarshal(msgBytes, &msg)
		if err != nil {
			slog.Error("Error decoding JSON:", "error", err)
			return
		}
		if rf.Role == "leader" {
			if msg.MsgType == SUBMIT_COMMAND_MSG {
				if msg.SubmitCommandRequest != nil && *msg.SubmitCommandRequest == "log" {
					fmt.Printf("\n\nEntries %+v\n\n", rf.Log.Entries)
				}

				rf.SubmitNewCommand(*msg.SubmitCommandRequest)
				rf.SendAppendEntriesToAllFollowers()
			}
		} else {
			if msg.MsgType == APPEND_ENTRIES_MSG {
				appendEntriesResult := handleAppendEntriesRequest(rf, msg)
				jsonData, err := json.Marshal(appendEntriesResult)
				if err != nil {
					slog.Error("Error encoding appendEntriesResult JSON:", "error", err)
					return
				}

				rf.send(conn.RemoteAddr().String(), jsonData)

				fmt.Printf("\n\n Log: %+v\n\n", rf.Log.Entries)
				// rf.send(SERVERS["1"], jsonData)
			}
		}

	}
}

func handleAppendEntriesRequest(rf *RaftLogic, msg Message) AppendEntriesResponse {
	// TODO: lock for the server's term
	serverCurrentTerm := rf.currentTerm

	// AppendEntries to our log
	if msg.AppendEntriesRequest == nil {
		panic("AppendEntriesRequest can't be nil")
	}
	serverMatchIdx, err := rf.Log.AppendEntries(*msg.AppendEntriesRequest)
	if err != nil {
		slog.Error("Error in AppendEntries", "error", err)
	}
	slog.Debug("AppendEntries result", "error", err)

	// Respond to leader with the AppendEntriesResult
	return AppendEntriesResponse{
		Success:                            err == nil,
		Term:                               serverCurrentTerm,
		MatchIndexFromAppendEntriesRequest: serverMatchIdx,
	}
}

// func (rf *RaftLogic) send(addr string, msg []byte) {
// 	conn, err := net.Dial("tcp", addr)
// 	if err != nil {
// 		slog.Error("Error connecting to server:", "error", err)
// 		return
// 	}
// 	defer conn.Close()

// 	// _, err = conn.Write(jsonData)
// 	err = Send(conn, msg)
// 	if err != nil {
// 		slog.Error("Error sending message:", "error", err)
// 		return
// 	}
// 	slog.Info("Message sent successfully")
// }

func (rf *RaftLogic) SendAppendEntriesToAllFollowers() {
	if rf.Role != "leader" {
		panic("Only leader can send AppendEtries to followers")
	}
	// For each follower
	for nodename, _ := range SERVERS {
		if nodename == rf.Nodename {
			continue
		}

		go rf.SendAppendEntryToFollower(nodename)
	}
	fmt.Println("SendAppendEntriesToAllFollowers() done")
}

func (rf *RaftLogic) SendAppendEntryToFollower(nodename string) {
	rf.Log.Lock()
	defer rf.Log.UnLock()
	// TODO: Implement this

	followerNextIdx := rf.nextIdxs[nodename]

	var prevLogIdx int
	if len(rf.Log.Entries) == 0 {
		prevLogIdx = 0
	} else {
		prevLogIdx = rf.Log.Entries[followerNextIdx-1].Idx
	}

	var prevLogTerm int
	if len(rf.Log.Entries) == 0 {
		prevLogTerm = rf.currentTerm
	} else {
		if prevLogIdx == -1 { // Special case for first entry :(
			prevLogTerm = 0
		} else {
			prevLogTerm = rf.Log.Entries[prevLogIdx].Term
		}
	}

	entries := Entries{}
	if len(rf.Log.Entries[followerNextIdx:]) > 0 {
		entries = rf.Log.GetEntriesCopyUNSAFE(followerNextIdx)
	}

	params := AppendEntriesRequest{
		LeaderTerm: rf.currentTerm,
		LeaderID:   rf.Nodename, // we only send AppendEntries if we're the leader

		PrevLogIdx:  prevLogIdx,
		PrevLogTerm: prevLogTerm,

		Entries: entries,

		LeaderCommitIdx: rf.commitIdx,
	}

	message := Message{
		MsgType:              APPEND_ENTRIES_MSG,
		AppendEntriesRequest: &params,
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		slog.Error("Error encoding JSON:", "error", err)
		return
	}

	fmt.Printf("\nSERVERS[nodename]: %s\njsonData:\n%+v\n", nodename, string(jsonData))

	rf.send(SERVERS[nodename], jsonData)
	// res, err := rf.sendAndRcv(SERVERS[nodename], jsonData)
	// if err != nil {
	// 	slog.Error("Error sending message:", "error", err)
	// 	return
	// }

	// fmt.Printf("\nFollower response: %+v\n\n", string(res))
}

func (rf *RaftLogic) send(addr string, msg []byte) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		// slog.Error("Error connecting to server:", "error", err)
		return
	}
	defer conn.Close()

	fmt.Printf("\nmsg: %s\n\n", msg)
	// _, err = conn.Write(jsonData)
	err = Send(conn, msg)
	if err != nil {
		slog.Error("Error sending message:", "error", err)
		return
	}
	slog.Info("Message sent successfully")
}

func (rf *RaftLogic) sendAndRcv(addr string, msg []byte) ([]byte, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		// slog.Error("Error connecting to server:", "error", err)
		return nil, err
	}
	defer conn.Close()

	// _, err = conn.Write(jsonData)
	err = Send(conn, msg)
	if err != nil {
		slog.Error("Error sending message:", "error", err)
		return nil, err
	}
	// slog.Info("Message sent successfully")

	res, err := Rcv_msg(conn)
	if err != nil {
		slog.Error("Error sending message:", "error", err)
		return nil, err
	}

	return res, nil
}

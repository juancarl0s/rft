package rft

import (
	"encoding/json"
	"fmt"
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
	nextIdx  map[string]int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	macthIds map[string]int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

func NewRaftLogic(nodename string, role string, term int) *RaftLogic {
	rf := &RaftLogic{
		Nodename:    nodename,
		clusterSize: len(SERVERS),

		Role: role,

		currentTerm: term,
		Log:         NewLog(),

		commitIdx:      0,
		lastAppliedIdx: 0,

		nextIdx:  map[string]int{},
		macthIds: map[string]int{},
	}

	// Populate nextIdx and matchIds for each server
	for name := range SERVERS {
		if name == nodename {
			continue
		}
		rf.nextIdx[name] = 1
		rf.macthIds[name] = 0
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

func (rf *RaftLogic) ReceiveAppendEntries(appendEntries AppendEntriesParams) {
	if appendEntries.LeaderTerm < rf.currentTerm {
		// TODO: Reply false
		return
	} else if appendEntries.LeaderTerm > rf.currentTerm {
		rf.currentTerm = appendEntries.LeaderTerm
	}

	// Append entries to log
	rf.Log.AppendEntries(appendEntries)
}

func (rf *RaftLogic) receive() {
	addr, exists := SERVERS[rf.Nodename]
	if !exists {
		panic(fmt.Sprintf("Node '%s' not found in SERVERS", rf.Nodename))
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("Error binding to port", "error", err)
		panic(err)
	}
	defer listener.Close()

	for {
		// Accept a new connection
		conn, err := listener.Accept()
		if err != nil {
			slog.Error("Error accepting connection:", "error", err)
			continue
		}
		addr := conn.RemoteAddr().String()
		slog.Debug("Connection from", "addr", addr)

		go rf.handleMsg(conn)
	}
}

func (rf *RaftLogic) handleMsg(conn net.Conn) {
	defer conn.Close()

	for {
		msg, err := Rcv_msg(conn)
		if err != nil {
			slog.Error("Error receiving message:", "error", err)
			continue
		}

		var params AppendEntriesParams
		err = json.Unmarshal(msg, &params)
		if err != nil {
			slog.Error("Error decoding JSON:", "error", err)
			return
		}

		err = rf.Log.AppendEntries(params)
		if err != nil {
			slog.Error("Error in AppendEntries", "error", err)
		}
		slog.Debug("AppendEntries result", "error", err)
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
		panic("Only leader can send AppendEntries to followers")
	}
	// For each follower
	for name, addr := range SERVERS {
		if name == rf.Nodename {
			continue
		}

		// TODO: Implement this
		nextIdx := rf.nextIdx[name]
		entries := rf.Log.GetEntries(nextIdx)
		go rf.SendAppendEntryToFollower(addr, AppendEntriesParams{
			Entries: entries,
		})
	}
	fmt.Println("SendAppendEntriesToAllFollowers() done")
}

func (rf *RaftLogic) SendAppendEntryToFollower(addr string, params AppendEntriesParams) {
	jsonData, err := json.Marshal(params)
	if err != nil {
		slog.Error("Error encoding JSON:", "error", err)
		return
	}

	rf.send(addr, jsonData)
}

func (rf *RaftLogic) send(addr string, msg []byte) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		// slog.Error("Error connecting to server:", "error", err)
		return
	}
	defer conn.Close()

	// _, err = conn.Write(jsonData)
	err = Send(conn, msg)
	if err != nil {
		slog.Error("Error sending message:", "error", err)
		return
	}
	// slog.Info("Message sent successfully")
}

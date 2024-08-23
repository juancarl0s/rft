package rft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"
)

// TODO: apply commands and update lastAppliedIdx accoirdingly

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

	volatileStateLock sync.Mutex //Used when changing commitIdx and lastAppliedIdx

	stateMachineCommandHandler CommandHandler
}

type CommandHandler interface {
	HandleCommand(cmd string) (string, error)
	String() string
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

		nextIdxs:  map[string]int{},
		macthIdxs: map[string]int{},

		volatileStateLock: sync.Mutex{},
	}

	rf.Log.AppendCommand(0, "initial_dummy_command arg")

	// Populate nextIdx and matchIds for each server
	for name := range SERVERS {
		// if name == nodename {
		// 	continue
		// }
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
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()
	if rf.Role != "leader" {
		panic("Only leader can send AppendEntries to followers")
	}
	rf.nextIdxs[rf.Nodename]++
	rf.macthIdxs[rf.Nodename]++

	// rf.lastAppliedIdx = rf.Log.AppendCommand(rf.currentTerm, cmd)
	_ = rf.Log.AppendCommand(rf.currentTerm, cmd)
}

func (rf *RaftLogic) Listen(listener net.Listener, stateMachineCommandHandler CommandHandler) {
	rf.stateMachineCommandHandler = stateMachineCommandHandler

	go rf.Heartbeats(3 * time.Second)

	for {
		// Accept a new connection
		conn, err := listener.Accept()
		if err != nil {
			slog.Error("Error accepting connection:", "error", err)
			continue
		}

		go rf.HandleIncomingMsg(conn)
	}
}

func (rf *RaftLogic) Heartbeats(duration time.Duration) {
	ticker := time.NewTicker(duration)
	for {
		select {
		case <-ticker.C:
			if rf.Role == "leader" {
				slog.Info("Sending heartbeats")
				rf.Commit()
				rf.runStateMatchineCommands()
				rf.Beat()
			}
		}
	}
}

func (rf *RaftLogic) Beat() {
	for nodename, _ := range SERVERS {
		if nodename == rf.Nodename {
			continue
		}
		go rf.SendAppendEntryToFollower(nodename)
	}
}

func (rf *RaftLogic) HandleIncomingMsg(conn net.Conn) {
	defer conn.Close()

	for {
		msgBytes, err := Rcv_msg(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				err = fmt.Errorf("error receiving message: %w", err)
				slog.Error("Error sending back response:", "error", err)
			}
			return
		}

		var msg Message
		err = json.Unmarshal(msgBytes, &msg)
		if err != nil {
			slog.Error("Error decoding JSON:", "error", err)
			return
		}

		if msg.MsgType == SUBMIT_COMMAND_MSG && msg.SubmitCommandRequest != nil && *msg.SubmitCommandRequest == "log" {
			fmt.Printf("\n\nLog.Entries:\n%+v\n\n", rf.Log.Entries)
			fmt.Printf("RaftServer:\n%+v\n\n", rf)
			fmt.Printf("KVStore:\n%+v\n\n", rf.stateMachineCommandHandler.String())
			if rf.Role == "leader" {
				rf.ForwardLogCommandToFollowers()
			}
			continue
		}

		if rf.Role == "leader" {
			if msg.MsgType == SUBMIT_COMMAND_MSG && msg.SubmitCommandRequest != nil {
				_, err := rf.stateMachineCommandHandler.HandleCommand(*msg.SubmitCommandRequest)
				if err != nil {
					slog.Error("Error running state machine command", "error", err)
					Send(conn, []byte("BAD"))
					continue
				}
				rf.SubmitNewCommand(*msg.SubmitCommandRequest)
				Send(conn, []byte("OK"))
				// rf.SendAppendEntriesToAllFollowers()

			} else if msg.MsgType == APPEND_ENTRIES_RESPONSE_MSG {
				rf.handleAppendEntriesResponse(*msg.AppendEntriesResponse)
			} else {
				slog.Error("Invalid message type", "msgType", msg.MsgType, "msg", msg)
			}

		} else if rf.Role == "follower" {
			if msg.MsgType == APPEND_ENTRIES_MSG {
				// fmt.Printf("\nReceived message:\n%+v\n\n", msg.AppendEntriesRequest)
				appendEntriesResult := rf.handleAppendEntriesRequest(msg)
				rf.SendAppendEntriesResponse(appendEntriesResult)
			}
		}

	}
}

// AppenEntries to local log
func (rf *RaftLogic) SendAppendEntriesResponse(res AppendEntriesResponse) {
	msg := Message{
		MsgType:               APPEND_ENTRIES_RESPONSE_MSG,
		AppendEntriesResponse: &res,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		slog.Error("Error encoding appendEntriesResult JSON:", "error", err)
		return
	}

	rf.send(SERVERS[res.NodenameFromRequest], msgBytes)
}

func (rf *RaftLogic) handleAppendEntriesResponse(res AppendEntriesResponse) {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()
	if rf.currentTerm < res.Term {
		// TODO:  handle this
		panic("NOT IMPLEMENTED YET - become follower? call for election?")
	}

	if rf.Role != "leader" {
		//TODO: handle this, this is not really true
		panic("Only leader can receive AppendEntries to followers")
	}

	if res.Success {
		resNextIdx := res.MatchIndexFromAppendEntriesRequest + 1
		resMatchIdx := res.MatchIndexFromAppendEntriesRequest

		currentNextIdx := rf.nextIdxs[res.NodenameWhereProcessed]
		currentMatchIdx := rf.macthIdxs[res.NodenameWhereProcessed]

		if currentMatchIdx < resMatchIdx {
			rf.macthIdxs[res.NodenameWhereProcessed] = resMatchIdx
		}
		if currentNextIdx < resNextIdx {
			rf.nextIdxs[res.NodenameWhereProcessed] = resNextIdx
		}
	}
}

func (rf *RaftLogic) Commit() {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()

	nodeMatchIdx := []int{}
	for _, idx := range rf.macthIdxs {
		nodeMatchIdx = append(nodeMatchIdx, idx)
	}

	sort.Ints(nodeMatchIdx)

	minimunMatchIdxInMajority := nodeMatchIdx[len(nodeMatchIdx)/2]

	if rf.commitIdx >= minimunMatchIdxInMajority {
		slog.Info("No index to commit")
		return
	}

	rf.commitIdx = minimunMatchIdxInMajority
	slog.Info("Index committed", "rf.commitIdx", rf.commitIdx, "minimunMatchIdxInMajority", minimunMatchIdxInMajority)
}

func (rf *RaftLogic) handleAppendEntriesRequest(msg Message) AppendEntriesResponse {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()

	// TODO: This is prob wrong, I must check if the terms are less than or more than.
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

	// Update last applied index (this is why we need to lock)
	// rf.lastAppliedIdx = serverMatchIdx

	// Update commit index of server
	if msg.AppendEntriesRequest.LeaderCommitIdx > rf.commitIdx {
		// Run commands in state machine
		rf.commitIdx = msg.AppendEntriesRequest.LeaderCommitIdx
	}

	fmt.Println("========== commitIdx, lastAppliedIdx", rf.commitIdx, rf.lastAppliedIdx)
	rf.runStateMatchineCommands()

	// Respond to leader with the AppendEntriesResult
	return AppendEntriesResponse{
		Success:                            err == nil,
		Term:                               serverCurrentTerm,
		MatchIndexFromAppendEntriesRequest: serverMatchIdx,
		NodenameFromRequest:                msg.AppendEntriesRequest.LeaderID,
		NodenameWhereProcessed:             rf.Nodename,
	}
}

func (rf *RaftLogic) runStateMatchineCommands() {
	if rf.commitIdx > rf.lastAppliedIdx {
		cmdsToRun := rf.Log.GetEntriesSlice(rf.lastAppliedIdx, rf.commitIdx+1)

		for _, cmd := range cmdsToRun {
			fmt.Printf("\nCMD: %+v", cmd)
			_, err := rf.stateMachineCommandHandler.HandleCommand(cmd.Cmd)
			if err != nil {
				slog.Error("Error running state machine command", "error", err)
				break
			}
			rf.lastAppliedIdx = cmd.Idx
		}
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
}

func (rf *RaftLogic) SendAppendEntryToFollower(nodename string) {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()
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
		entries = rf.Log.GetEntriesFromCopy(followerNextIdx)

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

	// fmt.Printf("\nSERVERS[nodename]: %s\njsonData:\n%+v\n", nodename, string(jsonData))

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

	// fmt.Printf("\nmsg: %s\n\n", msg)
	// _, err = conn.Write(jsonData)
	err = Send(conn, msg)
	if err != nil {
		slog.Error("Error sending message:", "error", err)
		return
	}
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

// func (rf *RaftLogic) Commit(idx int) {
// 	rf.volatileStateLock.Lock()
// 	defer rf.volatileStateLock.Unlock()

// 	if idx <= rf.commitIdx {
// 		defer slog.Info("No index to commit", "idxToCommit", idx, "commitIdx", rf.commitIdx)
// 		return
// 	}
// 	defer slog.Info("Index committed", "idx", idx)

// 	rf.commitIdx = idx
// }

func (rf *RaftLogic) ForwardLogCommandToFollowers() {
	if rf.Role != "leader" {
		panic("Only leader can forward log command to followers")
	}
	// For each follower
	for nodename, _ := range SERVERS {
		if nodename == rf.Nodename {
			continue
		}
		cmd := "log"
		msg := Message{
			MsgType:              SUBMIT_COMMAND_MSG,
			SubmitCommandRequest: &cmd,
		}
		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}
		go rf.send(SERVERS[nodename], jsonMsg)
	}
}

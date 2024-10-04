package rft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"math/rand"
)

type RaftLogic struct {
	// Cluster config details
	Nodename    string
	clusterSize int

	Role string

	// Persistent state on all servers
	currentTerm int // initialized to 0 on first boot, increases monotonically
	Log         *Log

	// Volatile state on all servers
	commitIdx      int // initialized to 0, increases monotonically
	lastAppliedIdx int // index of highest log entry applied to state machine, initialized to 0, increases monotonically

	// Volatile state on leaders
	nextIdxs  map[string]int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	macthIdxs map[string]int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Voting stuff
	votesForMe int // Number of votes received
	// votedFor          map[string]struct{} // By nodename
	votedFor          string     // By nodename
	volatileStateLock sync.Mutex // Used when changing commitIdx and lastAppliedIdx

	stateMachineCommandHandler CommandHandler

	timerToCallForElection         *time.Ticker
	timerDurationToCallForElection time.Duration
}

type candidacy struct {
	votedFor string
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

		votesForMe: 0,
		votedFor:   "",
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

func (rf *RaftLogic) BecomeCandidateAnRequestVotesUNSAFE() {
	// rf.volatileStateLock.Lock()

	rf.currentTerm++

	// Vote for self
	rf.votesForMe = 1
	rf.votedFor = ""

	rf.Role = "candidate"

	// rf.volatileStateLock.Unlock()

	for nodename, _ := range SERVERS {
		if nodename == rf.Nodename {
			continue
		}
		go rf.SendVoteRequest(nodename)
	}
}

func (rf *RaftLogic) SendVoteRequest(nodename string) {
	req := VoteRequest{
		Term:        rf.currentTerm,
		CandidateID: rf.Nodename,
		LastLogIdx:  rf.Log.Len() - 1,
		LastLogTerm: rf.Log.LastTerm(),
	}
	msg := Message{
		MsgType:     VOTE_REQUEST_MSG,
		VoteRequest: &req,
	}
	jsonData, err := json.Marshal(msg)
	if err != nil {
		slog.Error("Error encoding JSON:", "error", err)
		return
	}

	rf.send(SERVERS[nodename], jsonData)
}

func (rf *RaftLogic) BecomeFollowerUNSAFE() {
	// rf.volatileStateLock.Lock()
	// defer rf.volatileStateLock.Unlock()

	rf.Role = "follower"
}

func (rf *RaftLogic) BecomeLeaderUNSAFE() {
	// rf.volatileStateLock.Lock()
	// defer rf.volatileStateLock.Unlock()

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

	if strings.HasPrefix(cmd, "set") || strings.HasPrefix(cmd, "delete") || strings.HasPrefix(cmd, "snapshot") || strings.HasPrefix(cmd, "restore") {
		_ = rf.Log.AppendCommand(rf.currentTerm, cmd)
	}
}

func (rf *RaftLogic) ElectionCalling() {
	rf.volatileStateLock.Lock()
	rf.timerDurationToCallForElection = generateRandomElectionCallingDuration()
	rf.timerToCallForElection = time.NewTicker(rf.timerDurationToCallForElection)
	rf.volatileStateLock.Unlock()
	for {
		select {
		case <-rf.timerToCallForElection.C:
			if rf.Role != "leader" {
				rf.volatileStateLock.Lock()
				// rf.BecomeCandidateAnRequestVotesUNSAFE()
				// rf.timerDurationToCallForElection = generateRandomElectionCallingDuration()
				// rf.timerToCallForElection.Reset(rf.timerDurationToCallForElection)
				rf.CallForElectionUNSAFE()
				rf.volatileStateLock.Unlock()

			}
		}
	}
}

func (rf *RaftLogic) CallForElectionUNSAFE() {
	slog.Info("Calling for election", "forTerm", rf.currentTerm, "newElectionTimeout", rf.timerDurationToCallForElection)
	rf.BecomeCandidateAnRequestVotesUNSAFE()
	rf.timerDurationToCallForElection = generateRandomElectionCallingDuration()
	rf.timerToCallForElection.Reset(rf.timerDurationToCallForElection)
}

func (rf *RaftLogic) Heartbeats(duration time.Duration) {
	ticker := time.NewTicker(duration)
	for {
		select {
		case <-ticker.C:
			if rf.Role == "leader" {
				fmt.Println("♥", time.Now())
				rf.Commit()
				rf.runStateMatchineCommands()
				rf.sendAppendEntries()
			}
		}
	}
}

func (rf *RaftLogic) sendAppendEntries() {
	for nodename, _ := range SERVERS {
		if nodename == rf.Nodename {
			continue
		}
		go rf.sendAppendEntry(nodename)
	}
}

func generateRandomDuration(min, max int) time.Duration {
	return time.Duration(rand.Intn(max+min)+min) * time.Second
}
func generateRandomElectionCallingDuration() time.Duration {
	return generateRandomDuration(1, 10)
}

func (rf *RaftLogic) Listen(listener net.Listener, stateMachineCommandHandler CommandHandler) {
	rf.stateMachineCommandHandler = stateMachineCommandHandler

	go rf.Heartbeats(1 * time.Second)

	go rf.ElectionCalling()

	fmt.Println("------------------ LOG ------------------------")
	fmt.Printf("\nLog.Entries:\n%+v\n", rf.Log.Entries)
	fmt.Println("------------------ KVSTORE --------------------")
	fmt.Printf("KVStore:\n%+v\n", rf.stateMachineCommandHandler.String())
	fmt.Println("------------------ SERVER ---------------------")
	fmt.Printf("RaftServer:\n%+v\n", rf)
	fmt.Println("-----------------------------------------------")

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
			fmt.Println("------------------ LOG ------------------------")
			fmt.Printf("\nLog.Entries:\n%+v\n", rf.Log.Entries)
			fmt.Println("------------------ KVSTORE --------------------")
			fmt.Printf("KVStore:\n%+v\n", rf.stateMachineCommandHandler.String())
			fmt.Println("------------------ SERVER ---------------------")
			fmt.Printf("RaftServer:\n%+v\n", rf)
			fmt.Println("-----------------------------------------------")
			if rf.Role == "leader" {
				rf.ForwardLogCommandToEveryoneElse()
			}
			Send(conn, []byte("OK"))
			continue
		}

		if msg.MsgType == VOTE_REQUEST_MSG {
			// fmt.Printf("\nVoteRequest:\n%+v\n\n", msg.VoteRequest)

			// fmt.Printf("\n1<__________msg %+v\n", *msg.VoteRequest)
			// fmt.Println("-------------------------------")

			res := rf.handleVoteRequest(*msg.VoteRequest)
			// fmt.Printf("\n1res %+v\n__________>1", res)
			rf.SendVoteResponse(msg.VoteRequest.CandidateID, res)
			continue
		}

		if rf.Role == "leader" {
			if msg.MsgType == SUBMIT_COMMAND_MSG && msg.SubmitCommandRequest != nil {
				result, err := rf.stateMachineCommandHandler.HandleCommand(*msg.SubmitCommandRequest)
				if err != nil {
					slog.Error("Error running state machine command", "error", err)
					Send(conn, []byte("BAD"))
				} else {
					rf.SubmitNewCommand(*msg.SubmitCommandRequest)
					rf.lastAppliedIdx++
					if result != "" {
						Send(conn, []byte(result))
					} else {
						Send(conn, []byte("OK"))
					}
				}

				// rf.SendAppendEntriesToAllFollowers()
			} else if msg.MsgType == APPEND_ENTRIES_RESPONSE_MSG {
				// fmt.Printf("\n========== msg %+v\n", *msg.AppendEntriesResponse)
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

			if msg.MsgType == SUBMIT_COMMAND_MSG && msg.SubmitCommandRequest != nil {
				if rf.Role != "leader" {
					Send(conn, []byte("NOT LEADER"))
				}
				continue
			}
		} else if rf.Role == "candidate" {
			if msg.MsgType == VOTE_RESPONSE_MSG {
				// fmt.Printf("\nVoteResponse:\n%+v\n\n", msg.VoteResponse)

				rf.handleVoteResponse(*msg.VoteResponse)
			}

			if msg.MsgType == APPEND_ENTRIES_MSG {

				// fmt.Printf("\nReceived message:\n%+v\n\n", msg.AppendEntriesRequest)
				appendEntriesResult := rf.handleAppendEntriesRequest(msg)
				rf.SendAppendEntriesResponse(appendEntriesResult)
			}
		}
	}
}

func (rf *RaftLogic) handleVoteResponse(res VoteResponse) {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()

	if res.Term > rf.currentTerm {
		rf.currentTerm = res.Term
		rf.BecomeFollowerUNSAFE()
	}

	if res.VoteGranted {
		rf.votesForMe++
	}

	if rf.votesForMe > (rf.clusterSize/2)+1 {
		rf.BecomeLeaderUNSAFE()
	}
}

func (rf *RaftLogic) handleVoteRequest(req VoteRequest) VoteResponse {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()

	// if I'm a candidate, check my term vs voterequest term
	//  my term bigger -> deny vote
	//  my term lower -> become follower and vote

	if req.Term < rf.currentTerm {
		return VoteResponse{
			Term:        rf.currentTerm,
			VoteGranted: false,
			VoterID:     rf.Nodename,
		}
	}

	if req.Term > rf.currentTerm {
		rf.currentTerm = req.Term
		rf.BecomeFollowerUNSAFE()
		rf.votedFor = ""
	}

	if rf.votedFor == "" {
		rf.votedFor = req.CandidateID
		if req.Term >= rf.currentTerm {
			rf.timerToCallForElection.Reset(rf.timerDurationToCallForElection)
			slog.Info("Voted", "for", rf.votedFor)
			return VoteResponse{
				Term:        rf.currentTerm,
				VoteGranted: true,
				VoterID:     rf.Nodename,
			}
		}
	}
	return VoteResponse{
		Term:        rf.currentTerm,
		VoteGranted: false,
		VoterID:     rf.Nodename,
	}
}

func (rf *RaftLogic) SendVoteResponse(candidateId string, res VoteResponse) {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()

	msg := Message{
		MsgType:      VOTE_RESPONSE_MSG,
		VoteResponse: &res,
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		slog.Error("Error encoding appendEntriesResult JSON:", "error", err)
		return
	}

	rf.send(SERVERS[candidateId], msgBytes)

	return
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
	// fmt.Printf("\n========== msgBytes %+v\n", string(msgBytes))
	rf.send(SERVERS[res.NodenameFromRequest], msgBytes)
}

// TODO: implement follower -> leader redirect for client calls to follower

func (rf *RaftLogic) handleAppendEntriesResponse(res AppendEntriesResponse) {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()
	if rf.currentTerm < res.Term {
		rf.currentTerm = res.Term
		rf.BecomeFollowerUNSAFE()
		return
		// // TODO:  handle this
		// panic("NOT IMPLEMENTED YET - become follower? call for election?")
	}

	if rf.Role != "leader" {
		return
		//TODO: handle this, this is not really true
		// panic("Only leader can receive AppendEntries to followers")
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
	} else {
		resNextIdx := res.MatchIndexFromAppendEntriesRequest + 1
		resMatchIdx := res.MatchIndexFromAppendEntriesRequest
		// fmt.Printf("\n===@@======= resMatchIdx %+v\n", resMatchIdx)

		rf.macthIdxs[res.NodenameWhereProcessed] = resMatchIdx
		rf.nextIdxs[res.NodenameWhereProcessed] = resNextIdx
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

	// The median index match (highest log entry index known to be replicated on server)
	// across all servers is the index to commit across servers, because
	// the majority of the servers have it in their log.
	maximumMatchIdxInMajority := nodeMatchIdx[len(nodeMatchIdx)/2]

	// If we're here is because we are (or think we are the leader),
	// if the maximumMatchIdxInMajority is higher than our log's max index
	// (which is the same as .Len()), we're somehow behind so we call for an election.
	if rf.Log.Len() < maximumMatchIdxInMajority {
		// TODO: juan
		fmt.Println("CALL AN ELECTION NOW!!!!") //?????
	}

	if rf.commitIdx >= maximumMatchIdxInMajority {
		// slog.Info("No index to commit")
		return
	}

	rf.commitIdx = maximumMatchIdxInMajority
	slog.Info("Index committed", "rf.commitIdx", rf.commitIdx, "minimunMatchIdxInMajority", maximumMatchIdxInMajority)
}

func (rf *RaftLogic) handleAppendEntriesRequest(msg Message) AppendEntriesResponse {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()

	// AppendEntries to our log
	if msg.AppendEntriesRequest == nil {
		panic("AppendEntriesRequest can't be nil")
	}

	if msg.AppendEntriesRequest.LeaderTerm >= rf.currentTerm {
		rf.timerToCallForElection.Reset(rf.timerDurationToCallForElection)
	}

	serverMatchIdx, err := rf.Log.AppendEntries(*msg.AppendEntriesRequest)

	if msg.AppendEntriesRequest.LeaderTerm < rf.currentTerm {
		rf.currentTerm = msg.AppendEntriesRequest.LeaderTerm
		if rf.Role != "follower" {
			rf.BecomeFollowerUNSAFE()
		}

		return AppendEntriesResponse{
			Success:                            false,
			Term:                               rf.currentTerm,
			MatchIndexFromAppendEntriesRequest: serverMatchIdx,
			NodenameFromRequest:                msg.AppendEntriesRequest.LeaderID,
			NodenameWhereProcessed:             rf.Nodename,
		}
	}

	if err != nil {
		slog.Warn("Error in AppendEntries", "reason", err)
		return AppendEntriesResponse{
			Success:                            err == nil,
			Term:                               rf.currentTerm,
			MatchIndexFromAppendEntriesRequest: serverMatchIdx,
			NodenameFromRequest:                msg.AppendEntriesRequest.LeaderID,
			NodenameWhereProcessed:             rf.Nodename,
		}
	}

	if msg.AppendEntriesRequest.LeaderTerm < rf.currentTerm {
		return AppendEntriesResponse{
			Success:                            false,
			Term:                               rf.currentTerm,
			MatchIndexFromAppendEntriesRequest: 0,
			NodenameFromRequest:                msg.AppendEntriesRequest.LeaderID,
			NodenameWhereProcessed:             rf.Nodename,
		}
	}

	// Update commit index of server
	if msg.AppendEntriesRequest.LeaderCommitIdx > rf.commitIdx && len(rf.Log.Entries) >= rf.commitIdx {
		// Run commands in state machine
		rf.commitIdx = msg.AppendEntriesRequest.LeaderCommitIdx
	}

	rf.runStateMatchineCommands()

	// Respond to leader with the AppendEntriesResult
	return AppendEntriesResponse{
		Success:                            err == nil,
		Term:                               rf.currentTerm,
		MatchIndexFromAppendEntriesRequest: serverMatchIdx,
		NodenameFromRequest:                msg.AppendEntriesRequest.LeaderID,
		NodenameWhereProcessed:             rf.Nodename,
	}
}

func (rf *RaftLogic) runStateMatchineCommands() {
	if rf.commitIdx > rf.lastAppliedIdx {
		cmdsToRun := rf.Log.GetEntriesSlice(rf.lastAppliedIdx, rf.commitIdx+1)

		for _, cmd := range cmdsToRun {
			_, err := rf.stateMachineCommandHandler.HandleCommand(cmd.Cmd)
			if err != nil {
				slog.Error("Error running state machine command", "error", err)
				break
			}
			rf.lastAppliedIdx = cmd.Idx
		}
	}
}

func (rf *RaftLogic) SendAppendEntriesToAllFollowers() {
	if rf.Role != "leader" {
		panic("Only leader can send AppendEtries to followers")
	}
	// For each follower
	for nodename, _ := range SERVERS {
		if nodename == rf.Nodename {
			continue
		}

		go rf.sendAppendEntry(nodename)
	}
}

func (rf *RaftLogic) sendAppendEntry(nodename string) {
	rf.volatileStateLock.Lock()
	defer rf.volatileStateLock.Unlock()

	// Defensive code to avoid race condisitons and out of index errors
	followerNextIdx := rf.nextIdxs[nodename]
	if followerNextIdx > len(rf.Log.Entries) {
		followerNextIdx = len(rf.Log.Entries)
		rf.nextIdxs[nodename] = followerNextIdx
	}

	// fmt.Printf("\n%+v\n", rf)
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

	rf.send(SERVERS[nodename], jsonData)
}

func (rf *RaftLogic) ForwardLogCommandToEveryoneElse() {
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

// func (rf *RaftLogic) sendAndRcv(addr string, msg []byte) ([]byte, error) {
// 	conn, err := net.Dial("tcp", addr)
// 	if err != nil {
// 		// slog.Error("Error connecting to server:", "error", err)
// 		return nil, err
// 	}
// 	defer conn.Close()

// 	// _, err = conn.Write(jsonData)
// 	err = Send(conn, msg)
// 	if err != nil {
// 		slog.Error("Error sending message:", "error", err)
// 		return nil, err
// 	}
// 	// slog.Info("Message sent successfully")

// 	res, err := Rcv_msg(conn)
// 	if err != nil {
// 		slog.Error("Error sending message:", "error", err)
// 		return nil, err
// 	}

// 	return res, nil
// }

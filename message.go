package rft

var (
	APPEND_ENTRIES_MSG          = "AppendEntriesRequest"
	APPEND_ENTRIES_RESPONSE_MSG = "AppendEntriesResponse"

	VOTE_REQUEST_MSG  = "VoteRequest"
	VOTE_RESPONSE_MSG = "VoteResponse"

	SUBMIT_COMMAND_MSG = "SubmitCommand"
)

// Every communication between servers in the Raft cluster is to be done via a Message (i.e. using the below type).
type Message struct {
	MsgType string `json:"messageType,omitempty"`

	SubmitCommandRequest  *string                `json:"submitCommand,omitempty"`
	AppendEntriesRequest  *AppendEntriesRequest  `json:"appendEntriesRequest,omitempty"`
	AppendEntriesResponse *AppendEntriesResponse `json:"appendEntriesResponse,omitempty"`
	VoteRequest           *VoteRequest           `json:"requestVote,omitempty"`
	VoteResponse          *VoteResponse          `json:"voteResponse,omitempty"`
}

type AppendEntriesResponse struct {
	Success                            bool   `json:"success"` // true if follower appended entries successfully
	Term                               int    `json:"term"`    // currentTerm, for leader to update itself
	MatchIndexFromAppendEntriesRequest int    `json:"matchIndex"`
	NodenameWhereProcessed             string `json:"nodenameWhereProcessed"`
	NodenameFromRequest                string `json:"nodenameFromRequest"`
}

type AppendEntriesRequest struct {
	LeaderTerm int    `json:"term"`     // Leader's term
	LeaderID   string `json:"leaderID"` // This is the leader ID that (must have) originated this request.

	PrevLogIdx  int `json:"prevLogIndex"` // Log index of the log entry immediately preceding new ones.
	PrevLogTerm int `json:"prevLogTerm"`  // Term of the log entry immediately preceding new ones.

	Entries Entries `json:"entries"` // Log entries to store (empty for heartbeat)

	LeaderCommitIdx int `json:"leaderCommit"` // (inclusive)
}

type VoteRequest struct {
	Term        int    `json:"term"`        // Candidate's term
	CandidateID string `json:"candidateID"` // Candidate requesting vote.

	LastLogIdx  int `json:"lastLogIndex"` // Index of candidate's last log entry.
	LastLogTerm int `json:"lastLogTerm"`  // Term of candidate's last log entry.
}

type VoteResponse struct {
	Term        int    `json:"term"`        // Current term, for candidate to update itself.
	VoteGranted bool   `json:"voteGranted"` // True means candidate received vote.
	VoterID     string `json:"voterID"`     // The ID of the server that granted the vote.
}

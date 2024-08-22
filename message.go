package rft

import "errors"

var (
	APPEND_ENTRIES_MSG = "AppendEntriesRequest"
	SUBMIT_COMMAND_MSG = "SubmitCommand"
)

type Message struct {
	MsgType               string                 `json:"messageType,omitempty"`
	SubmitCommandRequest  *string                `json:"submitCommand,omitempty"`
	AppendEntriesRequest  *AppendEntriesRequest  `json:"appendEntriesRequest,omitempty"`
	AppendEntriesResponse *AppendEntriesResponse `json:"appendEntriesResponse,omitempty"`
}

type AppendEntriesResponse struct {
	Success                            bool `json:"success"` // true if follower appended entries successfully
	Term                               int  `json:"term"`    // currentTerm, for leader to update itself
	MatchIndexFromAppendEntriesRequest int  `json:"matchIndex"`
}

type AppendEntriesRequest struct {
	LeaderTerm int    `json:"term"`     // Leader's term
	LeaderID   string `json:"leaderID"` // This is the leader ID that (must have) originated this request.

	PrevLogIdx  int `json:"prevLogIndex"` // Log index of the log entry immediately preceding new ones.
	PrevLogTerm int `json:"prevLogTerm"`  // Term of the log entry immediately preceding new ones.

	Entries Entries `json:"entries"` // Log entries to store (empty for heartbeat)

	LeaderCommitIdx int `json:"leaderCommit"` // (inclusive)
}

func (p AppendEntriesRequest) Valid() error {
	// Allow the initial case
	if len(p.Entries) > 0 {
		// if p.Entries[0].Idx == 0 {
		// 	return nil
		// }

		if p.LeaderCommitIdx >= p.Entries[0].Idx {
			return errors.New("LeaderCommitIdx must be less than EntriesIndexStart")
		}
	}

	return nil
}

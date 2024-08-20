package rft

import (
	"errors"
	"sync"
)

type Entry struct {
	Idx  int
	Term int

	Cmd string
	// CmdClientID string // later

	// Commited bool?
}

type Entries []Entry

type Log struct {
	CurrentTerm int // latest term server has seen, maybe this should be in the Server struct?
	// VotedFor	string // later

	Entries Entries
	Lock    *sync.Mutex

	EntryLookup map[int]*Entry
}

type AppendEntriesParams struct {
	Term     int
	LeaderID string

	PrevLogIdx  int // -1 for the initial case?
	PrevLogTerm int

	EntriesIdxStart int // use this 0 for initial case?
	Entries         Entries

	LeaderCommitIdx int // (inclusive)
}

func (p AppendEntriesParams) Valid() error {
	if p.LeaderCommitIdx >= p.EntriesIdxStart {
		return errors.New("LeaderCommitIdx must be less than EntriesIndexStart")
	}

	return nil
}

func (l *Log) AppendEntries(params AppendEntriesParams) error {
	if err := params.Valid(); err != nil {
		return err
	}

	l.Lock.Lock()
	defer l.Lock.Unlock()

	// not yet, focus on log
	// if params.Term > l.CurrentTerm {
	// 	l.CurrentTerm = params.Term
	// }

	// TODO: stuff
	isHeartbeat := len(params.Entries) == 0
	if isHeartbeat {
		// TODO: do some checks for heartbeat
	} else {
		// TODO: do some checks for heartbeat
		pass := l.passConsistencyCheck(params)
		if pass {
			l.Entries = append(l.Entries, params.Entries...)
		}
	}

	return nil
}

func (l *Log) passConsistencyCheck(params AppendEntriesParams) bool {
	latestEntry := l.LogLatestEntry()

	// 1) The log is never allowed to have holes in it
	if latestEntry.Idx != params.PrevLogIdx {
		return false
	}
	// 2) There is a log-continuity condition where every append operation must verify that
	// the term number of any previous entry matches an expected value
	if latestEntry.Term != params.PrevLogTerm {
		return false
	}

	// 3) Special case: Appending new log entries at the start of the log needs to work
	// TODO: REVISAR!
	if latestEntry == nil {
		if params.EntriesIdxStart == 0 {
			return false
		}
		return true
	}

	// 4) Append_entries() is "idempotent." That means that append_entries() can be called
	// repeatedly with the same arguments and the end result is always the same
	// 4.a) Inserting case
	if len(l.Entries) == params.EntriesIdxStart {
		return true
	}
	// 4.b) Upserting case

	// TODO: stuff
	return true
}

// func (l *Log) latestTermIndex() (term, index int) {
// 	return l.currentTerm(), l.latestIndex()
// }

// func (l *Log) currentTerm() int {
// 	if len(l.Entries) == 0 {
// 		return 0
// 	}
// 	return l.Entries[len(l.Entries)-1].Term
// }

func (l *Log) LogLatestEntry() *Entry {
	if len(l.Entries) == 0 {
		return nil
	}

	return &l.Entries[len(l.Entries)-1]
}

func (l *Log) latestIndex() int {
	if len(l.Entries) == 0 {
		return 0
	}
	return l.Entries[len(l.Entries)-1].Term
}

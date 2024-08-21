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
	// VotedFor	string // later, maybe this should be in the Server struct?

	Entries Entries
	Lock    sync.Mutex

	// EntryLookup map[int]*Entry
}

// func NewLog() *Log {
// 	log := &Log{
// 		CurrentTerm: 0,
// 		Entries:     Entries{},
// 		Lock:        sync.Mutex{},
// 	}

// 	return log
// }

type AppendEntriesParams struct {
	LeaderTerm int
	LeaderID   string

	PrevLogIdx  int // Log index of the log entry immediately preceding new ones.
	PrevLogTerm int // Term of the log entry immediately preceding new ones.

	// EntriesIdxStart int // use this 0 for initial case?
	Entries Entries

	LeaderCommitIdx int // (inclusive)
}

func (p AppendEntriesParams) Valid() error {
	// Allow the initial case
	if len(p.Entries) > 0 {
		if p.Entries[0].Idx == 0 {
			return nil
		}
		if p.LeaderCommitIdx >= p.Entries[0].Idx {
			return errors.New("LeaderCommitIdx must be less than EntriesIndexStart")
		}
	}

	return nil
}

func (l *Log) LogLatestEntry() *Entry {
	if len(l.Entries) == 0 {
		return nil
	}

	return &l.Entries[len(l.Entries)-1]
}

func (l *Log) AppendEntries(params AppendEntriesParams) (int, error) {
	// Update log's current term to the leader's term, if the leader's term is greater
	if params.LeaderTerm > l.CurrentTerm {
		l.CurrentTerm = params.LeaderTerm
	}

	l.Lock.Lock()
	defer l.Lock.Unlock()

	if err := params.Valid(); err != nil {
		return l.CurrentTerm, err
	}

	pass := l.passConsistencyCheck(params)
	if !pass {
		return l.CurrentTerm, errors.New("consistency check failed")
	}
	l.editEntries(params)

	return l.CurrentTerm, nil
}

func (l *Log) passConsistencyCheck(params AppendEntriesParams) bool {
	latestEntry := l.LogLatestEntry()

	// 3) Special case: Appending new log entries at the start of the log needs to work
	// TODO: REVISAR!
	if latestEntry == nil {
		if params.Entries[0].Idx == 0 {
			return true
		} else {
			return false
		}
	}

	// 1) The log is never allowed to have holes in it.
	// if latestEntry.Idx < params.PrevLogIdx  {
	if len(params.Entries) > 0 && params.Entries[0].Idx > latestEntry.Idx+1 {
		return false
	}

	// 2) There is a log-continuity condition where every append operation
	// must verify that the term number of any previous
	// entry matches an expected value
	prevEntry := l.Entries[params.PrevLogIdx]
	if prevEntry.Term != params.PrevLogTerm {
		return false
	}
	// if latestEntry.Term > params.PrevLogTerm {
	// 	return false
	// }

	return true
}

func (l *Log) editEntries(params AppendEntriesParams) {
	// 4) Append_entries() is "idempotent." That means that append_entries()
	// can be called repeatedly with the same arguments and the end result
	// is always the same

	// Heartbeat case
	if len(params.Entries) == 0 {
		return
	}

	entryToEditIdx := params.Entries[0].Idx

	var needTruncateRest bool
	for _, newEntry := range params.Entries {
		if len(l.Entries) > entryToEditIdx {
			// 4.a) In-place change case:
			existingEntry := l.Entries[entryToEditIdx]
			if existingEntry.Idx == newEntry.Idx && existingEntry.Term == newEntry.Term {
				entryToEditIdx++
			} else {
				needTruncateRest = true
				break
			}
		} else if len(l.Entries) == entryToEditIdx {
			// 4.b) Append case:
			l.Entries = append(l.Entries, newEntry)
			entryToEditIdx++
		}
	}

	// 4.c) Deletion case:
	// This is the index where we failed to match the new entry with the existing entry
	// so we need to truncate the rest of the log from the failed index
	if needTruncateRest && entryToEditIdx < len(l.Entries) {
		l.Entries = l.Entries[:entryToEditIdx]
		entryToEditIdx++
	}
}

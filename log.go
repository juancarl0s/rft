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

func NewLog() *Log {
	log := &Log{
		CurrentTerm: 0,
		Entries:     Entries{},
		Lock:        sync.Mutex{},
	}

	return log
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
	// Allow the initial case
	if p.EntriesIdxStart == 0 {
		return nil
	}
	if p.LeaderCommitIdx >= p.EntriesIdxStart {
		return errors.New("LeaderCommitIdx must be less than EntriesIndexStart")
	}

	return nil
}

func (l *Log) LogLatestEntry() *Entry {
	if len(l.Entries) == 0 {
		return nil
	}

	return &l.Entries[len(l.Entries)-1]
}

// type AppendEntriesResponse struct {
// }

func (l *Log) AppendEntries(params AppendEntriesParams) (int, error) {
	l.Lock.Lock()
	defer l.Lock.Unlock()

	if err := params.Valid(); err != nil {
		return l.CurrentTerm, err
	}

	l.heartbeat(params)

	pass := l.passConsistencyCheck(params)
	if !pass {
		return l.CurrentTerm, errors.New("consistency check failed")
	}
	l.editEntries(params)

	return l.CurrentTerm, nil
}

func (l *Log) heartbeat(params AppendEntriesParams) {
	if params.Term > l.CurrentTerm {
		l.CurrentTerm = params.Term
	}
}

func (l *Log) passConsistencyCheck(params AppendEntriesParams) bool {
	latestEntry := l.LogLatestEntry()

	// 3) Special case: Appending new log entries at the start of the log needs to work
	// TODO: REVISAR!
	if latestEntry == nil {
		if params.EntriesIdxStart == 0 {
			return true
		} else {
			return false
		}
	}

	// 1) The log is never allowed to have holes in it
	if latestEntry.Idx < params.PrevLogIdx {
		return false
	}

	// 2) There is a log-continuity condition where every append operation
	// must verify that the term number of any previous
	// entry matches an expected value
	prevEntry := l.Entries[params.PrevLogIdx]
	if prevEntry.Term <= params.PrevLogTerm {
		return false
	}
	// if latestEntry.Term > params.PrevLogTerm {
	// 	return false
	// }

	return true
}

func (l *Log) editEntries(params AppendEntriesParams) {
	// 4) Append_entries() is "idempotent." That means that append_entries() can be called
	// repeatedly with the same arguments and the end result is always the same
	// 4.b) Upserting case

	if len(params.Entries) == 0 {
		return
	}

	entryToEditIdx := params.EntriesIdxStart

	// this is not correct
	for _, entry := range params.Entries {
		// 4.a) In-place change case:
		if len(l.Entries) > entryToEditIdx {
			existingEntry := l.Entries[entryToEditIdx]
			if existingEntry.Idx == entry.Idx && existingEntry.Term == entry.Term {
				entryToEditIdx++
				continue
			} else { // if we find a mismatch, we need to delete the rest of the entries
				break
			}
		}

		// 4.b) Append case:
		if len(l.Entries) == entryToEditIdx {
			l.Entries = append(l.Entries, entry)
			entryToEditIdx++
			continue
		}

		panic("This should never happen")
	}

	// 4.c) Deletion case:
	if entryToEditIdx < len(l.Entries) {
		l.Entries = l.Entries[:entryToEditIdx]
		entryToEditIdx++
	}
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

// func (l *Log) latestIndex() int {
// 	if len(l.Entries) == 0 {
// 		return 0
// 	}
// 	return l.Entries[len(l.Entries)-1].Term
// }

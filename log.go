package rft

import (
	"fmt"
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
	// CurrentTerm int // latest term server has seen, maybe this should be in the Server struct?
	// VotedFor	string // later, maybe this should be in the Server struct?

	Entries     Entries
	EntriesLock sync.Mutex

	// EntryLookup map[int]*Entry
}

func NewLog() *Log {
	log := &Log{
		// CurrentTerm: 0,
		Entries:     Entries{},
		EntriesLock: sync.Mutex{},
	}

	return log
}

func (l *Log) LogLatestEntry() *Entry {
	if len(l.Entries) == 0 {
		return nil
	}

	return &l.Entries[len(l.Entries)-1]
}

func (l *Log) AppendCommand(term int, cmd string) {
	l.EntriesLock.Lock()
	defer l.EntriesLock.Unlock()

	var idx int

	latestEntry := l.LogLatestEntry()
	if latestEntry != nil {
		idx = latestEntry.Idx + 1
	}

	newEntry := Entry{
		Idx:  idx,
		Term: term,
		Cmd:  cmd,
	}
	l.Entries = append(l.Entries, newEntry)
}

func (l *Log) GetEntriesCopyUNSAFE(fromIdx int) Entries {
	entries := Entries{}
	for i := fromIdx; i < len(l.Entries); i++ {
		entries = append(entries, l.Entries[i])
	}

	return entries
}

func (l *Log) Lock() {
	l.EntriesLock.Lock()
}

func (l *Log) UnLock() {
	l.EntriesLock.Unlock()
}

// TODO: remove Current term from Log
// Return the MatchIndex for the leader to update and an error
func (l *Log) AppendEntries(params AppendEntriesRequest) (int, error) {
	// Move to RaftLogic
	// if params.LeaderTerm < l.CurrentTerm {
	// 	return l.CurrentTerm, errors.New("leader's term is less than current term")
	// }

	// Move to RaftLogic
	// Update log's current term to the leader's term, if the leader's term is greater
	// if params.LeaderTerm > l.CurrentTerm {
	// 	l.CurrentTerm = params.LeaderTerm
	// }

	l.EntriesLock.Lock()
	defer l.EntriesLock.Unlock()
	// defer fmt.Printf("\n\nAppendEntriesParams: %+v\n\n", params)
	// defer fmt.Printf("\n\n@@@@@@@@@@Log Entries: %+v\n\n", l.Entries)
	matchIndex := 0
	if len(l.Entries) > 0 {
		matchIndex = l.Entries[len(l.Entries)-1].Idx
	}
	if err := params.Valid(); err != nil {
		return matchIndex, fmt.Errorf("invalid AppendEntriesRequest: %w", err)
	}

	if err := l.consistencyCheck(params); err != nil {
		return matchIndex, fmt.Errorf("consistency check failed: %w", err)
	}
	l.editEntries(params)

	matchIndex = 0
	if len(l.Entries) > 0 {
		matchIndex = l.Entries[len(l.Entries)-1].Idx
	}
	return matchIndex, nil
}

func (l *Log) consistencyCheck(params AppendEntriesRequest) error {
	latestEntry := l.LogLatestEntry()

	// 3) Special case: Appending new log entries at the start of the log needs to work
	// TODO: REVISAR!
	if latestEntry == nil {
		if len(params.Entries) == 0 { //first appendEndtries (heartbeat)
			return nil
		} else if params.Entries[0].Idx == 0 { //first appendEndtries (non-heartbeat) with entries
			return nil
		} else {
			return fmt.Errorf("latestEntry is nil and expected the first entry to be 0 in: %+v", params.Entries)
		}
	}

	// 1) The log is never allowed to have holes in it.
	if len(params.Entries) > 0 && params.Entries[0].Idx > latestEntry.Idx+1 {
		return fmt.Errorf("log is never allowed to have holes in it. params.Entries[0].Idx: %+v, latestEntry.Idx: %+v", params.Entries[0].Idx, latestEntry.Idx)
	}

	// 2) There is a log-continuity condition where every append operation
	// must verify that the term number of any previous
	// entry matches an expected value
	prevEntry := l.Entries[params.PrevLogIdx]
	if prevEntry.Term != params.PrevLogTerm {
		return fmt.Errorf("term number of any previous entry doesn't match. prevEntry.Term: %+v, params.PrevLogTerm: %+v", prevEntry.Term, params.PrevLogTerm)
	}

	return nil
}

func (l *Log) editEntries(params AppendEntriesRequest) {
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

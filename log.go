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
}

type Entries []Entry

type Log struct {
	Entries     Entries
	EntriesLock sync.Mutex
	// EntryLookup map[int]*Entry // prob not
}

func NewLog() *Log {
	log := &Log{
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

func (l *Log) AppendCommand(term int, cmd string) int {
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

	return newEntry.Idx
}

func (l *Log) GetEntriesFromCopy(fromIdx int) Entries {
	l.EntriesLock.Lock()
	defer l.EntriesLock.Unlock()

	entries := Entries{}
	for i := fromIdx; i < len(l.Entries); i++ {
		entries = append(entries, l.Entries[i])
	}

	return entries
}

func (l *Log) GetEntriesSlice(fromIdx, toIdx int) Entries {
	l.EntriesLock.Lock()
	defer l.EntriesLock.Unlock()

	entries := Entries{}
	for _, entry := range l.Entries[fromIdx:toIdx] {
		entries = append(entries, entry)
	}

	return entries
}

func (l *Log) Len() int {
	l.EntriesLock.Lock()
	defer l.EntriesLock.Unlock()

	return len(l.Entries)
}

// AppendEntries handles the AppendEntriesRequest by performing several steps:
// 1. Locks the EntriesLock to ensure thread-safe access to the log entries.
// 2. Initializes matchIndex to 0. If there are existing entries, sets matchIndex to the index of the last entry.
// 3. Validates the AppendEntriesRequest. If invalid, returns the current matchIndex and an error.
// 4. Performs a consistency check on the request. If it fails, returns the current matchIndex and an error.
// 5. Edits the log entries based on the request.
// 6. Updates matchIndex to the index of the last entry if there are any entries.
// 7. Returns the updated matchIndex and nil error.
// Returns the MatchIndex for the leader to update its state.
func (l *Log) AppendEntries(params AppendEntriesRequest) (int, error) {
	l.EntriesLock.Lock()
	defer l.EntriesLock.Unlock()

	// Hack, it's safe for me to do this because I put a dummy record at the start of the log.
	if l.Entries[len(l.Entries)-1].Idx < params.PrevLogIdx {
		return l.Entries[len(l.Entries)-1].Idx, fmt.Errorf("error latestEntry.Idx < params.PrevLogIdx")
	}

	matchIndex := 0
	if len(l.Entries) > 0 {
		matchIndex = l.Entries[len(l.Entries)-1].Idx
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

	// // 3) Special case: Appending new log entries at the start of the log needs to work
	// // TODO: REVISAR!
	// if latestEntry == nil {
	// 	if len(params.Entries) == 0 { //first appendEndtries (heartbeat)
	// 		return nil
	// 	} else if params.Entries[0].Idx == 0 { //first appendEndtries (non-heartbeat) with entries
	// 		return nil
	// 	} else {
	// 		return fmt.Errorf("latestEntry is nil and expected the first entry to be 0 in: %+v", params.Entries)
	// 	}
	// }

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
	// is always the same.

	// Heartbeat case, no need to make changes.
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
	// so we need to truncate the rest of the log from the failed index.
	if needTruncateRest && entryToEditIdx < len(l.Entries) {
		l.Entries = l.Entries[:entryToEditIdx]
		entryToEditIdx++
	}
}

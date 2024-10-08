package rft

import (
	"reflect"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLog_LogLatestEntry(t *testing.T) {
	type fields struct {
		CurrentTerm int
		Entries     Entries
		Lock        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *Entry
	}{
		{
			name: "empty log",
			fields: fields{
				Entries: Entries{},
			},
			want: nil,
		},
		{
			name: "one entry log",
			fields: fields{
				Entries: Entries{
					{
						Idx:  0,
						Term: 1,
						Cmd:  "set 1 1",
					},
				},
			},
			want: &Entry{
				Idx:  0,
				Term: 1,
				Cmd:  "set 1 1",
			},
		},
		{
			name: "one entry log",
			fields: fields{
				Entries: Entries{
					{
						Idx:  0,
						Term: 1,
						Cmd:  "set 1 1",
					},
					{
						Idx:  1,
						Term: 1,
						Cmd:  "set 2 2",
					},
				},
			},
			want: &Entry{
				Idx:  1,
				Term: 1,
				Cmd:  "set 2 2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Log{
				Entries:     tt.fields.Entries,
				EntriesLock: tt.fields.Lock,
			}
			if got := l.LogLatestEntry(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Log.LogLatestEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLog_AppendEntries(t *testing.T) {
	tests := []struct {
		name         string
		log          *Log
		params       AppendEntriesRequest
		wantErr      bool
		wantEntries  Entries
		wantMatchIdx int
	}{
		// {
		// 	name: "invalidParams",
		// 	params: AppendEntriesRequest{
		// 		Entries:         Entries{{Idx: 1, Term: 1}},
		// 		LeaderCommitIdx: 10,
		// 	},
		// 	log: &Log{
		// 		Entries: Entries{{
		// 			Idx:  0,
		// 			Term: 0,
		// 			Cmd:  "dummy",
		// 		}},
		// 		EntriesLock: sync.Mutex{},
		// 	},
		// 	wantErr: true,
		// 	wantEntries: Entries{{
		// 		Idx:  0,
		// 		Term: 0,
		// 		Cmd:  "dummy",
		// 	}},
		// 	wantMatchIdx: 0,
		// },
		// Consistency check
		// {
		// 	name: "passConsistencyCheck/first_append/fail",
		// 	params: AppendEntriesRequest{
		// 		LeaderTerm: 1,
		// 		LeaderID:   "leaderID",

		// 		PrevLogIdx:  0,
		// 		PrevLogTerm: 0,

		// 		Entries: Entries{
		// 			{
		// 				Idx:  0,
		// 				Term: 0,
		// 				Cmd:  "dummy",
		// 			},
		// 			{
		// 				Idx:  1,
		// 				Term: 1,
		// 			},
		// 		},

		// 		LeaderCommitIdx: 0,
		// 	},
		// 	log: &Log{
		// 		Entries: Entries{
		// 			{
		// 				Idx:  0,
		// 				Term: 0,
		// 				Cmd:  "dummy",
		// 			},
		// 		},
		// 		EntriesLock: sync.Mutex{},
		// 	},
		// 	wantErr: true,
		// 	wantEntries: Entries{
		// 		{
		// 			Idx:  0,
		// 			Term: 0,
		// 			Cmd:  "dummy",
		// 		},
		// 	},
		// 	wantMatchIdx: 0,
		// },
		// {
		// 	name: "passConsistencyCheck/first_append/success",
		// 	params: AppendEntriesRequest{
		// 		LeaderTerm: 1,
		// 		LeaderID:   "leaderID",

		// 		PrevLogIdx:  0,
		// 		PrevLogTerm: 0,

		// 		Entries: Entries{
		// 			{
		// 				Idx:  0,
		// 				Term: 0,
		// 				Cmd:  "dummy",
		// 			},
		// 			{
		// 				Idx:  1,
		// 				Term: 1,
		// 			},
		// 		},

		// 		LeaderCommitIdx: 0,
		// 	},
		// 	log: &Log{
		// 		Entries: Entries{
		// 			{
		// 				Idx:  0,
		// 				Term: 0,
		// 				Cmd:  "dummy",
		// 			},
		// 		},
		// 		EntriesLock: sync.Mutex{},
		// 	},
		// 	wantEntries: Entries{
		// 		{
		// 			Idx:  0,
		// 			Term: 0,
		// 			Cmd:  "dummy",
		// 		},
		// 		{
		// 			Idx:  1,
		// 			Term: 1,
		// 		},
		// 	},
		// 	wantMatchIdx: 1,
		// },
		{
			name: "passConsistencyCheck/first_append/empty/success",
			params: AppendEntriesRequest{
				LeaderTerm: 1,
				LeaderID:   "leaderID",

				PrevLogIdx:  0,
				PrevLogTerm: 0,

				Entries: Entries{},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries: Entries{
					{
						Idx:  0,
						Term: 0,
						Cmd:  "dummy",
					},
				},
				EntriesLock: sync.Mutex{},
			},
			wantEntries: Entries{
				{
					Idx:  0,
					Term: 0,
					Cmd:  "dummy",
				},
			},
			wantMatchIdx: 0,
		},
		{
			name: "passConsistencyCheck/no_holes/fail",
			params: AppendEntriesRequest{
				LeaderTerm: 1,
				LeaderID:   "leaderID",

				PrevLogIdx:  1,
				PrevLogTerm: 1,

				Entries: Entries{
					{
						Idx:  5,
						Term: 1,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries: Entries{
					{
						Idx:  0,
						Term: 1,
						Cmd:  "dummy",
					},
					{
						Idx:  1,
						Term: 1,
					},
				},
				EntriesLock: sync.Mutex{},
			},
			wantErr: true,
			wantEntries: Entries{
				{
					Idx:  0,
					Term: 1,
					Cmd:  "dummy",
				},
				{
					Idx:  1,
					Term: 1,
				},
			},
			wantMatchIdx: 1,
		},
		{
			name: "passConsistencyCheck/no_holes_term/fail",
			params: AppendEntriesRequest{
				LeaderTerm: 2,
				LeaderID:   "leaderID",

				PrevLogIdx:  1,
				PrevLogTerm: 1,

				Entries: Entries{
					{
						Idx:  2,
						Term: 1,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries: Entries{
					{
						Idx:  0,
						Term: 2,
						Cmd:  "dummy",
					},
					{
						Idx:  1,
						Term: 2,
					},
				},
				EntriesLock: sync.Mutex{},
			},
			wantErr: true,
			wantEntries: Entries{
				{
					Idx:  0,
					Term: 2,
					Cmd:  "dummy",
				},
				{
					Idx:  1,
					Term: 2,
				},
			},
			wantMatchIdx: 1,
		},
		{
			name: "success",
			params: AppendEntriesRequest{
				LeaderTerm: 3,
				LeaderID:   "leaderID",

				PrevLogIdx:  1,
				PrevLogTerm: 2,

				Entries: Entries{
					{
						Idx:  2,
						Term: 3,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries: Entries{
					{
						Idx:  0,
						Term: 2,
						Cmd:  "dummy",
					},
					{
						Idx:  1,
						Term: 2,
					},
				},
				EntriesLock: sync.Mutex{},
			},
			wantEntries: Entries{
				{
					Idx:  0,
					Term: 2,
					Cmd:  "dummy",
				},
				{
					Idx:  1,
					Term: 2,
				},
				{
					Idx:  2,
					Term: 3,
				},
			},
			wantMatchIdx: 2,
		},
		{
			name: "editentries/heartbeat/success",
			params: AppendEntriesRequest{
				LeaderTerm: 3,
				LeaderID:   "leaderID",

				PrevLogIdx:  1,
				PrevLogTerm: 2,

				Entries: Entries{},

				LeaderCommitIdx: 1,
			},
			log: &Log{
				Entries: Entries{
					{
						Idx:  0,
						Term: 2,
						Cmd:  "dummy",
					},
					{
						Idx:  1,
						Term: 2,
					},
				},
				EntriesLock: sync.Mutex{},
			},
			wantEntries: Entries{
				{
					Idx:  0,
					Term: 2,
					Cmd:  "dummy",
				},
				{
					Idx:  1,
					Term: 2,
				},
			},
			wantMatchIdx: 1,
		},
		{
			name: "editentries/middle_of_log_edits/noop/success",
			params: AppendEntriesRequest{
				LeaderTerm: 3,
				LeaderID:   "leaderID",

				PrevLogIdx:  0,
				PrevLogTerm: 2,

				Entries: Entries{
					{
						Idx:  1,
						Term: 2,
					},
					{
						Idx:  2,
						Term: 2,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries: Entries{
					{
						Idx:  0,
						Term: 2,
						Cmd:  "dummy",
					},
					{
						Idx:  1,
						Term: 2,
					},
					{
						Idx:  2,
						Term: 2,
					},
					{
						Idx:  3,
						Term: 3,
					},
				},
				EntriesLock: sync.Mutex{},
			},
			wantEntries: Entries{
				{
					Idx:  0,
					Term: 2,
					Cmd:  "dummy",
				},
				{
					Idx:  1,
					Term: 2,
				},
				{
					Idx:  2,
					Term: 2,
				},
				{
					Idx:  3,
					Term: 3,
				},
			},
			wantMatchIdx: 3,
		},
		{
			name: "editentries/append_only/success",
			params: AppendEntriesRequest{
				LeaderTerm: 3,
				LeaderID:   "leaderID",

				PrevLogIdx:  3,
				PrevLogTerm: 3,

				Entries: Entries{
					{
						Idx:  4,
						Term: 3,
					},
					{
						Idx:  5,
						Term: 3,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries: Entries{
					{
						Idx:  0,
						Term: 2,
						Cmd:  "dummy",
					},
					{
						Idx:  1,
						Term: 2,
					},
					{
						Idx:  2,
						Term: 2,
					},
					{
						Idx:  3,
						Term: 3,
					},
				},
				EntriesLock: sync.Mutex{},
			},
			wantEntries: Entries{
				{
					Idx:  0,
					Term: 2,
					Cmd:  "dummy",
				},
				{
					Idx:  1,
					Term: 2,
				},
				{
					Idx:  2,
					Term: 2,
				},
				{
					Idx:  3,
					Term: 3,
				}, {
					Idx:  4,
					Term: 3,
				},
				{
					Idx:  5,
					Term: 3,
				},
			},
			wantMatchIdx: 5,
		},
		{
			name: "editentries/edit_some_delete_some/success",
			params: AppendEntriesRequest{
				LeaderTerm: 3,
				LeaderID:   "leaderID",

				PrevLogIdx:  0,
				PrevLogTerm: 2,

				Entries: Entries{
					{
						Idx:  1,
						Term: 2,
					},
					{
						Idx:  2,
						Term: 3,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries: Entries{
					{
						Idx:  0,
						Term: 2,
						Cmd:  "dummy",
					},
					{
						Idx:  1,
						Term: 2,
					},
					{
						Idx:  2,
						Term: 2,
					},
					{
						Idx:  3,
						Term: 3,
					},
				},
				EntriesLock: sync.Mutex{},
			},
			wantEntries: Entries{
				{
					Idx:  0,
					Term: 2,
					Cmd:  "dummy",
				},
				{
					Idx:  1,
					Term: 2,
				},
			},
			wantMatchIdx: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matchIdx, err := tt.log.AppendEntries(tt.params)
			if !cmp.Equal(tt.log.Entries, tt.wantEntries) {
				t.Errorf("Log.Entries = %v, wantEntries %v", tt.log.Entries, tt.wantEntries)
			}

			if (matchIdx) != tt.wantMatchIdx {
				t.Errorf("matchIdx = %v, wantMatchIdx %v", matchIdx, tt.wantMatchIdx)
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("Log.AppendEntries() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

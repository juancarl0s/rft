package rft

import (
	"reflect"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// func TestNewLog(t *testing.T) {
// 	tests := []struct {
// 		name string
// 		want *Log
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if got := NewLog(); !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("NewLog() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

func TestAppendEntriesParams_Valid(t *testing.T) {

	tests := []struct {
		name    string
		params  AppendEntriesParams
		wantErr bool
	}{
		{
			name: "1",
			params: AppendEntriesParams{
				Entries: Entries{{Idx: 0}},
			},
		},
		{
			name: "2",
			params: AppendEntriesParams{
				Entries:         Entries{{Idx: 1}},
				LeaderCommitIdx: 2,
			},
			wantErr: true,
		},
		{
			name: "3",
			params: AppendEntriesParams{
				Entries:         Entries{{Idx: 0}},
				LeaderCommitIdx: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.params.Valid(); (err != nil) != tt.wantErr {
				t.Errorf("AppendEntriesParams.Valid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

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
		name        string
		log         *Log
		params      AppendEntriesParams
		wantErr     bool
		wantEntries Entries
	}{
		{
			name: "invalidParams",
			params: AppendEntriesParams{
				Entries:         Entries{{Idx: 1}},
				LeaderCommitIdx: 10,
			},
			log: &Log{
				Entries:     Entries{},
				EntriesLock: sync.Mutex{},
			},
			wantErr:     true,
			wantEntries: Entries{},
		},
		// Consistency check
		{
			name: "passConsistencyCheck/first_append/fail",
			params: AppendEntriesParams{
				LeaderTerm: 1,
				LeaderID:   "leaderID",

				PrevLogIdx:  0,
				PrevLogTerm: 0,

				Entries: Entries{
					{
						Idx: 1,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries:     Entries{},
				EntriesLock: sync.Mutex{},
			},
			wantErr:     true,
			wantEntries: Entries{},
		},
		{
			name: "passConsistencyCheck/first_append/success",
			params: AppendEntriesParams{
				LeaderTerm: 1,
				LeaderID:   "leaderID",

				PrevLogIdx:  0,
				PrevLogTerm: 0,

				Entries: Entries{
					{
						Idx: 0,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries:     Entries{},
				EntriesLock: sync.Mutex{},
			},
			wantEntries: Entries{
				{
					Idx: 0,
				},
			},
		},
		{
			name: "passConsistencyCheck/first_append/empty/success",
			params: AppendEntriesParams{
				LeaderTerm: 1,
				LeaderID:   "leaderID",

				PrevLogIdx:  0,
				PrevLogTerm: 0,

				Entries: Entries{},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				Entries:     Entries{},
				EntriesLock: sync.Mutex{},
			},
			wantEntries: Entries{},
		},
		{
			name: "passConsistencyCheck/no_holes/fail",
			params: AppendEntriesParams{
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
				},
				{
					Idx:  1,
					Term: 1,
				},
			},
		},
		{
			name: "passConsistencyCheck/no_holes/fail",
			params: AppendEntriesParams{
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
				},
				{
					Idx:  1,
					Term: 2,
				},
			},
		},
		{
			name: "success",
			params: AppendEntriesParams{
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
		},
		{
			name: "editentries/heartbeat/success",
			params: AppendEntriesParams{
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
				},
				{
					Idx:  1,
					Term: 2,
				},
			},
		},
		{
			name: "editentries/middle_of_log_edits/noop/success",
			params: AppendEntriesParams{
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
		},
		{
			name: "editentries/append_only/success",
			params: AppendEntriesParams{
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
		},
		{
			name: "editentries/edit_some_delete_some/success",
			params: AppendEntriesParams{
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
				},
				{
					Idx:  1,
					Term: 2,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.log.AppendEntries(tt.params)
			if !cmp.Equal(tt.log.Entries, tt.wantEntries) {
				t.Errorf("Log.CurrentTerm = %v, wantTerm %v", tt.log.Entries, tt.wantEntries)
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("Log.AppendEntries() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// func TestLog_heartbeat(t *testing.T) {
// 	type fields struct {
// 		CurrentTerm int
// 		Entries     Entries
// 		Lock        sync.Mutex
// 	}
// 	type args struct {
// 		params AppendEntriesParams
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		args   args
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			l := &Log{
// 				CurrentTerm: tt.fields.CurrentTerm,
// 				Entries:     tt.fields.Entries,
// 				Lock:        tt.fields.Lock,
// 			}
// 			l.heartbeat(tt.args.params)
// 		})
// 	}
// }

func TestLog_passConsistencyCheck(t *testing.T) {
	type fields struct {
		CurrentTerm int
		Entries     Entries
		Lock        sync.Mutex
	}
	type args struct {
		params AppendEntriesParams
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Log{
				Entries:     tt.fields.Entries,
				EntriesLock: tt.fields.Lock,
			}
			if got := l.passConsistencyCheck(tt.args.params); got != tt.want {
				t.Errorf("Log.passConsistencyCheck() = %v, want %v", got, tt.want)
			}
		})
	}
}

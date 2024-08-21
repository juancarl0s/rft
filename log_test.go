package rft

import (
	"reflect"
	"sync"
	"testing"
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
	type fields struct {
		Term            int
		LeaderID        string
		PrevLogIdx      int
		PrevLogTerm     int
		EntriesIdxStart int
		Entries         Entries
		LeaderCommitIdx int
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "LeaderCommitIdx must be less than EntriesIndexStart",
			fields: fields{
				LeaderCommitIdx: 10,
				EntriesIdxStart: 5,
			},
			wantErr: true,
		},
		{
			name: "LeaderCommitIdx must not be equal to EntriesIndexStart",
			fields: fields{
				LeaderCommitIdx: 10,
				EntriesIdxStart: 10,
			},
			wantErr: true,
		},
		{
			name: "LeaderCommitIdx must not be equal to EntriesIndexStart",
			fields: fields{
				LeaderCommitIdx: 10,
				EntriesIdxStart: 11,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := AppendEntriesParams{
				Term:            tt.fields.Term,
				LeaderID:        tt.fields.LeaderID,
				PrevLogIdx:      tt.fields.PrevLogIdx,
				PrevLogTerm:     tt.fields.PrevLogTerm,
				EntriesIdxStart: tt.fields.EntriesIdxStart,
				Entries:         tt.fields.Entries,
				LeaderCommitIdx: tt.fields.LeaderCommitIdx,
			}
			if err := p.Valid(); (err != nil) != tt.wantErr {
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
				CurrentTerm: tt.fields.CurrentTerm,
				Entries:     tt.fields.Entries,
				Lock:        tt.fields.Lock,
			}
			if got := l.LogLatestEntry(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Log.LogLatestEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLog_AppendEntries(t *testing.T) {
	tests := []struct {
		name     string
		log      *Log
		params   AppendEntriesParams
		wantTerm int
		wantErr  bool
	}{
		{
			name: "invalidParams",
			params: AppendEntriesParams{
				LeaderCommitIdx: 10,
				EntriesIdxStart: 1,
			},
			log: &Log{
				CurrentTerm: 1,
				Entries:     Entries{},
				Lock:        sync.Mutex{},
			},
			wantTerm: 1,
			wantErr:  true,
		},
		// Consistency check
		{
			name: "passConsistencyCheck/first_append/fail",
			params: AppendEntriesParams{
				Term:     1,
				LeaderID: "leaderID",

				PrevLogIdx:  0,
				PrevLogTerm: 0,

				EntriesIdxStart: 1,
				Entries: Entries{
					{
						Idx: 1,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				CurrentTerm: 1,
				Entries:     Entries{},
				Lock:        sync.Mutex{},
			},
			wantTerm: 1,
			wantErr:  true,
		},
		{
			name: "passConsistencyCheck/first_append/success",
			params: AppendEntriesParams{
				Term:     1,
				LeaderID: "leaderID",

				PrevLogIdx:  0,
				PrevLogTerm: 0,

				EntriesIdxStart: 0,
				Entries: Entries{
					{
						Idx: 1,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				CurrentTerm: 1,
				Entries:     Entries{},
				Lock:        sync.Mutex{},
			},
			wantTerm: 1,
		},
		{
			name: "passConsistencyCheck/no_holes/fail",
			params: AppendEntriesParams{
				Term:     1,
				LeaderID: "leaderID",

				PrevLogIdx:  4,
				PrevLogTerm: 1,

				EntriesIdxStart: 2,
				Entries: Entries{
					{
						Idx:  5,
						Term: 1,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				CurrentTerm: 1,
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
				Lock: sync.Mutex{},
			},
			wantTerm: 1,
			wantErr:  true,
		},
		{
			name: "passConsistencyCheck/no_holes/fail",
			params: AppendEntriesParams{
				Term:     1,
				LeaderID: "leaderID",

				PrevLogIdx:  1,
				PrevLogTerm: 2,

				EntriesIdxStart: 2,
				Entries: Entries{
					{
						Idx:  2,
						Term: 1,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				CurrentTerm: 1,
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
				Lock: sync.Mutex{},
			},
			wantTerm: 1,
			wantErr:  true,
		},
		{
			name: "success",
			params: AppendEntriesParams{
				Term:     3,
				LeaderID: "leaderID",

				PrevLogIdx:  1,
				PrevLogTerm: 2,

				EntriesIdxStart: 2,
				Entries: Entries{
					{
						Idx:  2,
						Term: 3,
					},
				},

				LeaderCommitIdx: 0,
			},
			log: &Log{
				CurrentTerm: 1,
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
				Lock: sync.Mutex{},
			},
			wantTerm: 3,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			term, err := tt.log.AppendEntries(tt.params)
			if term != tt.wantTerm {
				t.Errorf("Log.CurrentTerm = %v, wantTerm %v", term, tt.wantTerm)
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
				CurrentTerm: tt.fields.CurrentTerm,
				Entries:     tt.fields.Entries,
				Lock:        tt.fields.Lock,
			}
			if got := l.passConsistencyCheck(tt.args.params); got != tt.want {
				t.Errorf("Log.passConsistencyCheck() = %v, want %v", got, tt.want)
			}
		})
	}
}

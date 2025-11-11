package raft

type LogEntry struct {
	Index uint64
	Term  uint64
	Cmd   []byte
}

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []LogEntry
}

type VolatileState struct {
	CommitIndex uint64
	LastApplied uint64
}

type LeaderState struct {
	NextIndex  map[string]uint64
	MatchIndex map[string]uint64
}


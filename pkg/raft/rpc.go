package raft

type RequestVoteArgs struct {
	Term         uint64
	CandidateId  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderId          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64
	SnapshotValid bool
	Snapshot     []byte
	SnapshotTerm uint64
	SnapshotIndex uint64
}


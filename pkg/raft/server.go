package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/virajbhartiya/raft/pkg/transport"
)

type Storage interface {
	SaveTerm(term uint64) error
	SaveVotedFor(votedFor string) error
	SaveLog(entries []LogEntry) error
	Load() (*PersistentState, error)
	SaveSnapshot(data []byte, lastIndex uint64, lastTerm uint64) error
	LoadSnapshot() ([]byte, uint64, uint64, error)
	AppendLogEntry(entry LogEntry) error
	Close() error
}

type Server struct {
	id        string
	mu        sync.Mutex
	persist   Storage
	ps        PersistentState
	vs        VolatileState
	ls        *LeaderState
	transport transport.Transport
	applyCh   chan<- ApplyMsg

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	stopCh          chan struct{}
	peers           []string
	state           string
}

func NewServer(id string, peers []string, persist Storage, t transport.Transport, applyCh chan<- ApplyMsg) *Server {
	s := &Server{
		id:        id,
		persist:   persist,
		transport: t,
		applyCh:   applyCh,
		peers:     peers,
		state:     "follower",
		stopCh:    make(chan struct{}),
	}

	state, err := persist.Load()
	if err == nil && state != nil {
		s.ps = *state
		fmt.Printf("[%s] Loaded persistent state: term=%d, votedFor=%s, logLength=%d\n", s.id, s.ps.CurrentTerm, s.ps.VotedFor, len(s.ps.Log))
	}

	s.resetElectionTimer()
	fmt.Printf("[%s] Server initialized, election timer set\n", s.id)
	return s
}

func (s *Server) resetElectionTimer() {
	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	s.electionTimer = time.NewTimer(timeout)
}

func (s *Server) becomeFollower(term uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = "follower"
	s.ps.CurrentTerm = term
	s.ps.VotedFor = ""
	s.persist.SaveTerm(term)
	s.persist.SaveVotedFor("")
	s.resetElectionTimer()
	if s.ls != nil {
		s.ls = nil
	}
	fmt.Printf("[%s] Became follower in term %d\n", s.id, term)
}

func (s *Server) becomeCandidate() {
	fmt.Printf("[%s] becomeCandidate() called\n", s.id)
	s.mu.Lock()
	fmt.Printf("[%s] becomeCandidate() acquired lock\n", s.id)
	s.state = "candidate"
	s.ps.CurrentTerm++
	s.ps.VotedFor = s.id
	fmt.Printf("[%s] becomeCandidate() updating state, calling SaveTerm\n", s.id)
	s.persist.SaveTerm(s.ps.CurrentTerm)
	fmt.Printf("[%s] becomeCandidate() SaveTerm done, calling SaveVotedFor\n", s.id)
	s.persist.SaveVotedFor(s.id)
	fmt.Printf("[%s] becomeCandidate() SaveVotedFor done, resetting timer\n", s.id)
	s.resetElectionTimer()
	fmt.Printf("[%s] Became candidate in term %d\n", s.id, s.ps.CurrentTerm)
	s.mu.Unlock()
}

func (s *Server) becomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = "leader"
	s.ls = &LeaderState{
		NextIndex:  make(map[string]uint64),
		MatchIndex: make(map[string]uint64),
	}
	lastLogIndex := uint64(len(s.ps.Log))
	for _, peer := range s.peers {
		s.ls.NextIndex[peer] = lastLogIndex + 1
		s.ls.MatchIndex[peer] = 0
	}
	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}
	if s.heartbeatTicker == nil {
		s.heartbeatTicker = time.NewTicker(50 * time.Millisecond)
	}
	fmt.Printf("[%s] Became leader in term %d\n", s.id, s.ps.CurrentTerm)
}

func (s *Server) isLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == "leader"
}

func (s *Server) IsLeader() bool {
	return s.isLeader()
}

func (s *Server) Start() {
	s.transport.RegisterHandler("RequestVote", s.handleRequestVote)
	s.transport.RegisterHandler("AppendEntries", s.handleAppendEntries)
	s.transport.RegisterHandler("InstallSnapshot", s.handleInstallSnapshot)
	go s.run()
}

func (s *Server) run() {
	fmt.Printf("[%s] Starting run loop\n", s.id)
	for {
		var heartbeatCh <-chan time.Time
		var electionCh <-chan time.Time

		s.mu.Lock()
		if s.heartbeatTicker != nil {
			heartbeatCh = s.heartbeatTicker.C
		}
		if s.electionTimer != nil {
			electionCh = s.electionTimer.C
		} else {
			fmt.Printf("[%s] WARNING: electionTimer is nil!\n", s.id)
			s.resetElectionTimer()
			electionCh = s.electionTimer.C
		}
		s.mu.Unlock()

		select {
		case <-electionCh:
			fmt.Printf("[%s] Election timeout fired\n", s.id)
			isLeader := s.isLeader()
			fmt.Printf("[%s] isLeader=%v\n", s.id, isLeader)
			if !isLeader {
				fmt.Printf("[%s] Calling startElection()\n", s.id)
				s.startElection()
			} else {
				fmt.Printf("[%s] Already leader, skipping election\n", s.id)
			}
		case <-heartbeatCh:
			if s.isLeader() {
				s.sendHeartbeats()
			}
		case <-s.stopCh:
			fmt.Printf("[%s] Run loop stopping\n", s.id)
			return
		}
	}
}

func (s *Server) Stop() {
	close(s.stopCh)
	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}
	if s.heartbeatTicker != nil {
		s.heartbeatTicker.Stop()
	}
}

func (s *Server) startElection() {
	fmt.Printf("[%s] startElection() called\n", s.id)
	s.becomeCandidate()
	fmt.Printf("[%s] After becomeCandidate(), calling sendRequestVotes()\n", s.id)
	s.sendRequestVotes()
	fmt.Printf("[%s] After sendRequestVotes()\n", s.id)
}

func (s *Server) sendRequestVotes() {
	s.mu.Lock()
	term := s.ps.CurrentTerm
	lastLogIndex := uint64(len(s.ps.Log))
	lastLogTerm := uint64(0)
	if lastLogIndex > 0 {
		lastLogTerm = s.ps.Log[lastLogIndex-1].Term
	}
	peers := s.peers
	s.mu.Unlock()

	fmt.Printf("[%s] Starting election: term=%d, lastLogIndex=%d, lastLogTerm=%d, peers=%v\n", s.id, term, lastLogIndex, lastLogTerm, peers)

	votes := 1
	votesCh := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(peerID string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			err := s.transport.SendRPC(peerID, "RequestVote", args, &reply)
			if err != nil {
				fmt.Printf("[%s] RequestVote to %s failed: %v\n", s.id, peerID, err)
				votesCh <- false
				return
			}
			if reply.VoteGranted && reply.Term == term {
				fmt.Printf("[%s] Received vote from %s\n", s.id, peerID)
				votesCh <- true
			} else {
				fmt.Printf("[%s] Vote denied by %s (term=%d, granted=%v)\n", s.id, peerID, reply.Term, reply.VoteGranted)
				votesCh <- false
			}
		}(peer)
	}

	for i := 0; i < len(peers); i++ {
		if <-votesCh {
			votes++
		}
		if votes > len(peers)/2 {
			fmt.Printf("[%s] Received majority votes (%d/%d)\n", s.id, votes, len(peers)+1)
			s.becomeLeader()
			s.sendHeartbeats()
			return
		}
	}
	fmt.Printf("[%s] Election failed: only %d votes\n", s.id, votes)
}

func (s *Server) sendHeartbeats() {
	s.mu.Lock()
	term := s.ps.CurrentTerm
	commitIndex := s.vs.CommitIndex
	peers := s.peers
	s.mu.Unlock()

	for _, peer := range peers {
		go func(peerID string) {
			s.sendAppendEntries(peerID, term, commitIndex)
		}(peer)
	}
}

func (s *Server) sendAppendEntries(peerID string, term uint64, leaderCommit uint64) {
	s.mu.Lock()
	nextIndex := s.ls.NextIndex[peerID]
	lastLogIndex := uint64(len(s.ps.Log))
	var entries []LogEntry
	if nextIndex <= lastLogIndex {
		entries = s.ps.Log[nextIndex-1:]
	}
	prevLogIndex := nextIndex - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 && prevLogIndex <= uint64(len(s.ps.Log)) {
		prevLogTerm = s.ps.Log[prevLogIndex-1].Term
	}
	s.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	var reply AppendEntriesReply
	err := s.transport.SendRPC(peerID, "AppendEntries", args, &reply)
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if reply.Term > s.ps.CurrentTerm {
		fmt.Printf("[%s] Higher term from %s: %d > %d\n", s.id, peerID, reply.Term, s.ps.CurrentTerm)
		s.becomeFollower(reply.Term)
		return
	}
	if reply.Success {
		oldMatchIndex := s.ls.MatchIndex[peerID]
		s.ls.MatchIndex[peerID] = prevLogIndex + uint64(len(entries))
		s.ls.NextIndex[peerID] = s.ls.MatchIndex[peerID] + 1
		if len(entries) > 0 && s.ls.MatchIndex[peerID] > oldMatchIndex {
			fmt.Printf("[%s] Replicated %d entries to %s (matchIndex=%d)\n", s.id, len(entries), peerID, s.ls.MatchIndex[peerID])
		}
		s.updateCommitIndex()
	} else {
		if s.ls.NextIndex[peerID] > 1 {
			s.ls.NextIndex[peerID]--
		}
	}
}

func (s *Server) updateCommitIndex() {
	oldCommitIndex := s.vs.CommitIndex
	for n := s.vs.CommitIndex + 1; n <= uint64(len(s.ps.Log)); n++ {
		if s.ps.Log[n-1].Term != s.ps.CurrentTerm {
			continue
		}
		count := 1
		for _, matchIndex := range s.ls.MatchIndex {
			if matchIndex >= n {
				count++
			}
		}
		if count > len(s.peers)/2 {
			s.vs.CommitIndex = n
		}
	}
	if s.vs.CommitIndex > oldCommitIndex {
		fmt.Printf("[%s] Updated commitIndex: %d -> %d\n", s.id, oldCommitIndex, s.vs.CommitIndex)
	}
	s.applyCommitted()
}

func (s *Server) applyCommitted() {
	for s.vs.LastApplied < s.vs.CommitIndex {
		s.vs.LastApplied++
		entry := s.ps.Log[s.vs.LastApplied-1]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Cmd,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}
		select {
		case s.applyCh <- msg:
		default:
		}
	}
}

func (s *Server) handleRequestVote(args interface{}) (interface{}, error) {
	var req RequestVoteArgs
	if m, ok := args.(map[string]interface{}); ok {
		if term, ok := m["Term"].(uint64); ok {
			req.Term = term
		}
		if id, ok := m["CandidateId"].(string); ok {
			req.CandidateId = id
		}
		if idx, ok := m["LastLogIndex"].(uint64); ok {
			req.LastLogIndex = idx
		}
		if term, ok := m["LastLogTerm"].(uint64); ok {
			req.LastLogTerm = term
		}
	} else {
		req = args.(RequestVoteArgs)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	reply := RequestVoteReply{
		Term:        s.ps.CurrentTerm,
		VoteGranted: false,
	}

	fmt.Printf("[%s] Received RequestVote from %s (term=%d, currentTerm=%d)\n", s.id, req.CandidateId, req.Term, s.ps.CurrentTerm)

	if req.Term > s.ps.CurrentTerm {
		s.state = "follower"
		s.ps.CurrentTerm = req.Term
		s.ps.VotedFor = ""
		s.persist.SaveTerm(req.Term)
		s.persist.SaveVotedFor("")
		s.resetElectionTimer()
		reply.Term = req.Term
	}

	if req.Term < s.ps.CurrentTerm {
		fmt.Printf("[%s] Rejecting RequestVote: stale term\n", s.id)
		return map[string]interface{}{
			"Term":        reply.Term,
			"VoteGranted": reply.VoteGranted,
		}, nil
	}

	if s.ps.VotedFor == "" || s.ps.VotedFor == req.CandidateId {
		lastLogIndex := uint64(len(s.ps.Log))
		lastLogTerm := uint64(0)
		if lastLogIndex > 0 {
			lastLogTerm = s.ps.Log[lastLogIndex-1].Term
		}
		if req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			s.ps.VotedFor = req.CandidateId
			s.persist.SaveVotedFor(req.CandidateId)
			s.resetElectionTimer()
			fmt.Printf("[%s] Granted vote to %s\n", s.id, req.CandidateId)
		} else {
			fmt.Printf("[%s] Rejecting RequestVote: log not up-to-date\n", s.id)
		}
	} else {
		fmt.Printf("[%s] Rejecting RequestVote: already voted for %s\n", s.id, s.ps.VotedFor)
	}

	return map[string]interface{}{
		"Term":        reply.Term,
		"VoteGranted": reply.VoteGranted,
	}, nil
}

func (s *Server) handleAppendEntries(args interface{}) (interface{}, error) {
	var req AppendEntriesArgs
	if m, ok := args.(map[string]interface{}); ok {
		if term, ok := m["Term"].(uint64); ok {
			req.Term = term
		}
		if id, ok := m["LeaderId"].(string); ok {
			req.LeaderId = id
		}
		if idx, ok := m["PrevLogIndex"].(uint64); ok {
			req.PrevLogIndex = idx
		}
		if term, ok := m["PrevLogTerm"].(uint64); ok {
			req.PrevLogTerm = term
		}
		if entries, ok := m["Entries"].([]interface{}); ok {
			req.Entries = make([]LogEntry, len(entries))
			for i, e := range entries {
				if entry, ok := e.(map[string]interface{}); ok {
					if idx, ok := entry["Index"].(uint64); ok {
						req.Entries[i].Index = idx
					}
					if term, ok := entry["Term"].(uint64); ok {
						req.Entries[i].Term = term
					}
					if cmd, ok := entry["Cmd"].([]byte); ok {
						req.Entries[i].Cmd = cmd
					}
				} else if entry, ok := e.(LogEntry); ok {
					req.Entries[i] = entry
				}
			}
		} else if entries, ok := m["Entries"].([]LogEntry); ok {
			req.Entries = entries
		}
		if commit, ok := m["LeaderCommit"].(uint64); ok {
			req.LeaderCommit = commit
		}
	} else {
		req = args.(AppendEntriesArgs)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	reply := AppendEntriesReply{
		Term:    s.ps.CurrentTerm,
		Success: false,
	}

	if req.Term > s.ps.CurrentTerm {
		s.state = "follower"
		s.ps.CurrentTerm = req.Term
		s.ps.VotedFor = ""
		s.persist.SaveTerm(req.Term)
		s.persist.SaveVotedFor("")
		s.resetElectionTimer()
		if s.ls != nil {
			s.ls = nil
		}
		reply.Term = req.Term
	}

	if req.Term < s.ps.CurrentTerm {
		return map[string]interface{}{
			"Term":    reply.Term,
			"Success": reply.Success,
		}, nil
	}

	s.resetElectionTimer()

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(s.ps.Log)) {
			return map[string]interface{}{
				"Term":    reply.Term,
				"Success": reply.Success,
			}, nil
		}
		if s.ps.Log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			if req.PrevLogIndex <= uint64(len(s.ps.Log)) {
				s.ps.Log = s.ps.Log[:req.PrevLogIndex-1]
				s.persist.SaveLog(s.ps.Log)
			}
			return map[string]interface{}{
				"Term":    reply.Term,
				"Success": reply.Success,
			}, nil
		}
	}

	if len(req.Entries) > 0 {
		s.ps.Log = append(s.ps.Log[:req.PrevLogIndex], req.Entries...)
		for i := req.PrevLogIndex; i < uint64(len(s.ps.Log)); i++ {
			s.ps.Log[i].Index = i + 1
			s.persist.AppendLogEntry(s.ps.Log[i])
		}
	}

	if req.LeaderCommit > s.vs.CommitIndex {
		if req.LeaderCommit < uint64(len(s.ps.Log)) {
			s.vs.CommitIndex = req.LeaderCommit
		} else {
			s.vs.CommitIndex = uint64(len(s.ps.Log))
		}
		s.applyCommitted()
	}

	reply.Success = true
	return map[string]interface{}{
		"Term":    reply.Term,
		"Success": reply.Success,
	}, nil
}

func (s *Server) handleInstallSnapshot(args interface{}) (interface{}, error) {
	var req InstallSnapshotArgs
	if m, ok := args.(map[string]interface{}); ok {
		if term, ok := m["Term"].(uint64); ok {
			req.Term = term
		}
		if id, ok := m["LeaderId"].(string); ok {
			req.LeaderId = id
		}
		if idx, ok := m["LastIncludedIndex"].(uint64); ok {
			req.LastIncludedIndex = idx
		}
		if term, ok := m["LastIncludedTerm"].(uint64); ok {
			req.LastIncludedTerm = term
		}
		if data, ok := m["Data"].([]byte); ok {
			req.Data = data
		}
	} else {
		req = args.(InstallSnapshotArgs)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	reply := InstallSnapshotReply{
		Term: s.ps.CurrentTerm,
	}

	if req.Term > s.ps.CurrentTerm {
		s.state = "follower"
		s.ps.CurrentTerm = req.Term
		s.ps.VotedFor = ""
		s.persist.SaveTerm(req.Term)
		s.persist.SaveVotedFor("")
		s.resetElectionTimer()
		if s.ls != nil {
			s.ls = nil
		}
		reply.Term = req.Term
	}

	if req.Term < s.ps.CurrentTerm {
		return map[string]interface{}{
			"Term": reply.Term,
		}, nil
	}

	if req.LastIncludedIndex > s.vs.CommitIndex {
		s.persist.SaveSnapshot(req.Data, req.LastIncludedIndex, req.LastIncludedTerm)
		if req.LastIncludedIndex < uint64(len(s.ps.Log)) {
			s.ps.Log = s.ps.Log[req.LastIncludedIndex:]
		} else {
			s.ps.Log = []LogEntry{}
		}
		s.vs.CommitIndex = req.LastIncludedIndex
		s.vs.LastApplied = req.LastIncludedIndex
		msg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      req.Data,
			SnapshotTerm:  req.LastIncludedTerm,
			SnapshotIndex: req.LastIncludedIndex,
		}
		select {
		case s.applyCh <- msg:
		default:
		}
	}

	return map[string]interface{}{
		"Term": reply.Term,
	}, nil
}

func (s *Server) StartCommand(command []byte) (uint64, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != "leader" {
		return 0, 0, false
	}

	entry := LogEntry{
		Index: uint64(len(s.ps.Log)) + 1,
		Term:  s.ps.CurrentTerm,
		Cmd:   command,
	}
	s.ps.Log = append(s.ps.Log, entry)
	s.persist.AppendLogEntry(entry)
	fmt.Printf("[%s] Appended command at index %d (term %d)\n", s.id, entry.Index, entry.Term)

	return entry.Index, entry.Term, true
}

func (s *Server) GetState() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]interface{}{
		"id":          s.id,
		"state":       s.state,
		"term":        s.ps.CurrentTerm,
		"votedFor":    s.ps.VotedFor,
		"commitIndex": s.vs.CommitIndex,
		"lastApplied": s.vs.LastApplied,
		"logLength":   len(s.ps.Log),
	}
}

func (s *Server) GetLeader() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == "leader" {
		return s.id
	}
	return ""
}

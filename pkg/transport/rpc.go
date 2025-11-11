package transport

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

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

type LogEntry struct {
	Index uint64
	Term  uint64
	Cmd   []byte
}

type StartReply struct {
	Index uint64
	Term  uint64
	OK    bool
}

type RPCTransport struct {
	mu       sync.RWMutex
	server   *rpc.Server
	listener net.Listener
	clients  map[string]*rpc.Client
	address  string
	handlers map[string]func(interface{}) (interface{}, error)
	stopCh   chan struct{}
	raftRPC  *RaftRPC
}

func NewRPCTransport(address string) *RPCTransport {
	raftRPC := &RaftRPC{
		handlers: make(map[string]func(interface{}) (interface{}, error)),
	}
	transport := &RPCTransport{
		server:   rpc.NewServer(),
		clients:  make(map[string]*rpc.Client),
		address:  address,
		handlers: make(map[string]func(interface{}) (interface{}, error)),
		stopCh:   make(chan struct{}),
		raftRPC:  raftRPC,
	}
	transport.server.RegisterName("Raft", raftRPC)
	return transport
}

func (t *RPCTransport) Start() error {
	listener, err := net.Listen("tcp", t.address)
	if err != nil {
		return err
	}
	t.listener = listener
	go func() {
		for {
			select {
			case <-t.stopCh:
				return
			default:
			}

			if tcpListener, ok := listener.(*net.TCPListener); ok {
				tcpListener.SetDeadline(time.Now().Add(100 * time.Millisecond))
			}

			conn, err := listener.Accept()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				select {
				case <-t.stopCh:
					return
				default:
					return
				}
			}
			go t.server.ServeConn(conn)
		}
	}()
	return nil
}

func (t *RPCTransport) RegisterHandler(method string, handler func(interface{}) (interface{}, error)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[method] = handler
	t.raftRPC.mu.Lock()
	t.raftRPC.handlers[method] = handler
	t.raftRPC.mu.Unlock()
}

func (t *RPCTransport) SendRPC(targetID string, method string, args interface{}, resp interface{}) error {
	t.mu.RLock()
	client, ok := t.clients[targetID]
	t.mu.RUnlock()

	if !ok {
		conn, err := net.Dial("tcp", targetID)
		if err != nil {
			return err
		}
		client = rpc.NewClient(conn)
		t.mu.Lock()
		t.clients[targetID] = client
		t.mu.Unlock()
	}

	return client.Call("Raft."+method, args, resp)
}

func (t *RPCTransport) Incoming() <-chan interface{} {
	return make(chan interface{})
}

func (t *RPCTransport) Stop() error {
	close(t.stopCh)

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.listener != nil {
		t.listener.Close()
	}

	for _, client := range t.clients {
		client.Close()
	}
	t.clients = make(map[string]*rpc.Client)

	return nil
}

type RaftRPC struct {
	mu       sync.RWMutex
	handlers map[string]func(interface{}) (interface{}, error)
}

func (r *RaftRPC) GetState(args struct{}, reply *map[string]interface{}) error {
	r.mu.RLock()
	handler, ok := r.handlers["GetState"]
	r.mu.RUnlock()
	if ok {
		result, err := handler(args)
		if err != nil {
			return err
		}
		*reply = result.(map[string]interface{})
		return nil
	}
	return nil
}

func (r *RaftRPC) GetLeader(args struct{}, reply *string) error {
	r.mu.RLock()
	handler, ok := r.handlers["GetLeader"]
	r.mu.RUnlock()
	if ok {
		result, err := handler(args)
		if err != nil {
			return err
		}
		*reply = result.(string)
		return nil
	}
	return nil
}

func (r *RaftRPC) Start(args []byte, reply *StartReply) error {
	r.mu.RLock()
	handler, ok := r.handlers["Start"]
	r.mu.RUnlock()
	if ok {
		result, err := handler(args)
		if err != nil {
			return err
		}
		if resultMap, ok := result.(map[string]interface{}); ok {
			if index, ok := resultMap["Index"].(uint64); ok {
				reply.Index = index
			}
			if term, ok := resultMap["Term"].(uint64); ok {
				reply.Term = term
			}
			if okVal, ok := resultMap["OK"].(bool); ok {
				reply.OK = okVal
			}
		}
		return nil
	}
	return nil
}

func (r *RaftRPC) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.RLock()
	handler, ok := r.handlers["RequestVote"]
	r.mu.RUnlock()
	if ok {
		argsMap := map[string]interface{}{
			"Term":         args.Term,
			"CandidateId":  args.CandidateId,
			"LastLogIndex": args.LastLogIndex,
			"LastLogTerm":  args.LastLogTerm,
		}
		result, err := handler(argsMap)
		if err != nil {
			return err
		}
		if resultMap, ok := result.(map[string]interface{}); ok {
			if term, ok := resultMap["Term"].(uint64); ok {
				reply.Term = term
			}
			if granted, ok := resultMap["VoteGranted"].(bool); ok {
				reply.VoteGranted = granted
			}
		}
		return nil
	}
	return nil
}

func (r *RaftRPC) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.RLock()
	handler, ok := r.handlers["AppendEntries"]
	r.mu.RUnlock()
	if ok {
		argsMap := map[string]interface{}{
			"Term":         args.Term,
			"LeaderId":     args.LeaderId,
			"PrevLogIndex": args.PrevLogIndex,
			"PrevLogTerm":  args.PrevLogTerm,
			"Entries":      args.Entries,
			"LeaderCommit": args.LeaderCommit,
		}
		result, err := handler(argsMap)
		if err != nil {
			return err
		}
		if resultMap, ok := result.(map[string]interface{}); ok {
			if term, ok := resultMap["Term"].(uint64); ok {
				reply.Term = term
			}
			if success, ok := resultMap["Success"].(bool); ok {
				reply.Success = success
			}
		}
		return nil
	}
	return nil
}

func (r *RaftRPC) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	r.mu.RLock()
	handler, ok := r.handlers["InstallSnapshot"]
	r.mu.RUnlock()
	if ok {
		argsMap := map[string]interface{}{
			"Term":              args.Term,
			"LeaderId":          args.LeaderId,
			"LastIncludedIndex": args.LastIncludedIndex,
			"LastIncludedTerm":  args.LastIncludedTerm,
			"Data":              args.Data,
		}
		result, err := handler(argsMap)
		if err != nil {
			return err
		}
		if resultMap, ok := result.(map[string]interface{}); ok {
			if term, ok := resultMap["Term"].(uint64); ok {
				reply.Term = term
			}
		}
		return nil
	}
	return nil
}

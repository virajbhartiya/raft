package simulator

import (
	"os"
	"sync"
	"time"

	"github.com/virajbhartiya/raft/pkg/fsm"
	"github.com/virajbhartiya/raft/pkg/raft"
	"github.com/virajbhartiya/raft/pkg/storage"
	"github.com/virajbhartiya/raft/pkg/transport"
)

type Cluster struct {
	mu         sync.Mutex
	transport  *transport.InProcTransport
	servers    map[string]*raft.Server
	storages   map[string]*storage.Storage
	fsms       map[string]fsm.FSM
	applyChs   map[string]chan raft.ApplyMsg
	dataDirs   map[string]string
	partitions map[string]bool
}

func NewCluster(nodeIDs []string) (*Cluster, error) {
	c := &Cluster{
		transport:  transport.NewInProcTransport(),
		servers:    make(map[string]*raft.Server),
		storages:   make(map[string]*storage.Storage),
		fsms:       make(map[string]fsm.FSM),
		applyChs:   make(map[string]chan raft.ApplyMsg),
		dataDirs:   make(map[string]string),
		partitions: make(map[string]bool),
	}

	for _, id := range nodeIDs {
		dataDir := "/tmp/raft-test-" + id
		os.RemoveAll(dataDir)
		os.MkdirAll(dataDir, 0755)

		store, err := storage.NewStorage(dataDir)
		if err != nil {
			return nil, err
		}

		applyCh := make(chan raft.ApplyMsg, 100)
		fsm := fsm.NewKVStore()

		peers := make([]string, 0)
		for _, peerID := range nodeIDs {
			if peerID != id {
				peers = append(peers, peerID)
			}
		}

		nodeTransport := transport.NewNodeTransport(id, c.transport)
		server := raft.NewServer(id, peers, store, nodeTransport, applyCh)

		c.storages[id] = store
		c.fsms[id] = fsm
		c.applyChs[id] = applyCh
		c.dataDirs[id] = dataDir
		c.servers[id] = server

		go c.applyLoop(id, applyCh, fsm)
	}

	return c, nil
}

func (c *Cluster) applyLoop(id string, applyCh chan raft.ApplyMsg, fsm fsm.FSM) {
	for msg := range applyCh {
		if msg.CommandValid {
			fsm.Apply(msg.Command)
		} else if msg.SnapshotValid {
			fsm.Restore(msg.Snapshot)
		}
	}
}

func (c *Cluster) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, server := range c.servers {
		server.Start()
	}
}

func (c *Cluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, server := range c.servers {
		server.Stop()
	}
	for _, store := range c.storages {
		store.Close()
	}
	for _, dataDir := range c.dataDirs {
		os.RemoveAll(dataDir)
	}
}

func (c *Cluster) Partition(nodeID string, isolated bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitions[nodeID] = isolated
	c.transport.Partition(nodeID, isolated)
}

func (c *Cluster) SetDropRate(rate float64) {
	c.transport.SetDropRate(rate)
}

func (c *Cluster) SetDelay(min, max time.Duration) {
	c.transport.SetDelay(min, max)
}

func (c *Cluster) Crash(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if server, ok := c.servers[nodeID]; ok {
		server.Stop()
		delete(c.servers, nodeID)
	}
}

func (c *Cluster) Restart(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, ok := c.storages[nodeID]
	if !ok {
		return nil
	}

	applyCh := c.applyChs[nodeID]
	kvFSM := c.fsms[nodeID]

	peers := make([]string, 0)
	for id := range c.servers {
		if id != nodeID {
			peers = append(peers, id)
		}
	}

	nodeTransport := transport.NewNodeTransport(nodeID, c.transport)
	server := raft.NewServer(nodeID, peers, store, nodeTransport, applyCh)
	c.servers[nodeID] = server
	server.Start()

	go c.applyLoop(nodeID, applyCh, kvFSM)

	return nil
}

func (c *Cluster) GetServer(nodeID string) *raft.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[nodeID]
}

func (c *Cluster) WaitForLeader(timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for id, server := range c.servers {
			if server != nil {
				c.mu.Unlock()
				if c.isLeader(id) {
					return id
				}
				c.mu.Lock()
			}
		}
		c.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func (c *Cluster) isLeader(nodeID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	server := c.servers[nodeID]
	if server == nil {
		return false
	}
	return server.IsLeader()
}

func (c *Cluster) GetFSM(nodeID string) fsm.FSM {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fsms[nodeID]
}

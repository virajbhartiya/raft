package transport

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

type InProcTransport struct {
	mu          sync.RWMutex
	nodes       map[string]*InProcNode
	handlers    map[string]map[string]func(interface{}) (interface{}, error)
	dropRate    float64
	delayMin    time.Duration
	delayMax    time.Duration
	incoming    chan interface{}
	partitions  map[string]map[string]bool
}

type InProcNode struct {
	id       string
	transport *InProcTransport
}

type Message struct {
	From    string
	To      string
	Method  string
	Args    interface{}
	RespCh  chan interface{}
	ErrCh   chan error
}

func NewInProcTransport() *InProcTransport {
	return &InProcTransport{
		nodes:      make(map[string]*InProcNode),
		handlers:   make(map[string]map[string]func(interface{}) (interface{}, error)),
		incoming:   make(chan interface{}, 100),
		partitions: make(map[string]map[string]bool),
	}
}

func (t *InProcTransport) RegisterNode(id string) *InProcNode {
	t.mu.Lock()
	defer t.mu.Unlock()

	node := &InProcNode{
		id:        id,
		transport: t,
	}
	t.nodes[id] = node
	t.handlers[id] = make(map[string]func(interface{}) (interface{}, error))
	return node
}

func (t *InProcTransport) SetDropRate(rate float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropRate = rate
}

func (t *InProcTransport) SetDelay(min, max time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delayMin = min
	t.delayMax = max
}

func (t *InProcTransport) Partition(nodeID string, isolated bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.partitions[nodeID] == nil {
		t.partitions[nodeID] = make(map[string]bool)
	}
	t.partitions[nodeID]["isolated"] = isolated
}

func (t *InProcTransport) IsPartitioned(from, to string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if p, ok := t.partitions[from]; ok && p["isolated"] {
		return true
	}
	if p, ok := t.partitions[to]; ok && p["isolated"] {
		return true
	}
	return false
}

func (n *InProcNode) SendRPC(targetID string, method string, args interface{}, resp interface{}) error {
	t := n.transport
	t.mu.RLock()
	_, exists := t.nodes[targetID]
	handlers := t.handlers[targetID]
	dropRate := t.dropRate
	delayMin := t.delayMin
	delayMax := t.delayMax
	t.mu.RUnlock()

	if !exists {
		return errors.New("target node not found")
	}

	if t.IsPartitioned(n.id, targetID) {
		return errors.New("network partition")
	}

	if rand.Float64() < dropRate {
		return errors.New("message dropped")
	}

	handler, ok := handlers[method]
	if !ok {
		return errors.New("handler not found")
	}

	delay := delayMin
	if delayMax > delayMin {
		delay += time.Duration(rand.Int63n(int64(delayMax - delayMin)))
	}
	if delay > 0 {
		time.Sleep(delay)
	}

	result, err := handler(args)
	if err != nil {
		return err
	}

	if resp != nil {
		if respPtr, ok := resp.(*interface{}); ok {
			*respPtr = result
		}
	}

	return nil
}

func (n *InProcNode) RegisterHandler(method string, handler func(interface{}) (interface{}, error)) {
	t := n.transport
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[n.id][method] = handler
}

func (n *InProcNode) Incoming() <-chan interface{} {
	return n.transport.incoming
}

func (t *InProcTransport) SendRPC(fromID, targetID string, method string, args interface{}, resp interface{}) error {
	node, exists := t.nodes[fromID]
	if !exists {
		return errors.New("source node not found")
	}
	return node.SendRPC(targetID, method, args, resp)
}

func (t *InProcTransport) RegisterHandler(nodeID, method string, handler func(interface{}) (interface{}, error)) {
	node, exists := t.nodes[nodeID]
	if !exists {
		return
	}
	node.RegisterHandler(method, handler)
}

func (t *InProcTransport) Incoming() <-chan interface{} {
	return t.incoming
}


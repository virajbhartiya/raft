package fsm

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (kv *KVStore) Apply(command []byte) []byte {
	var cmd struct {
		Op    string
		Key   string
		Value string
	}
	dec := gob.NewDecoder(bytes.NewReader(command))
	if err := dec.Decode(&cmd); err != nil {
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case "set":
		kv.data[cmd.Key] = cmd.Value
		return []byte("OK")
	case "get":
		val, ok := kv.data[cmd.Key]
		if !ok {
			return []byte("")
		}
		return []byte(val)
	case "delete":
		delete(kv.data, cmd.Key)
		return []byte("OK")
	default:
		return nil
	}
}

func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(kv.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (kv *KVStore) Restore(snapshot []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	dec := gob.NewDecoder(bytes.NewReader(snapshot))
	if err := dec.Decode(&kv.data); err != nil {
		return err
	}
	return nil
}


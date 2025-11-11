package storage

import (
	"encoding/gob"
	"fmt"
	"os"
	"sync"

	"github.com/virajbhartiya/raft/pkg/raft"
)

type Storage struct {
	mu          sync.Mutex
	wal         *WAL
	stateFile   string
	snapshotDir string
}

func NewStorage(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir+"/snapshots", 0755); err != nil {
		return nil, err
	}

	wal, err := NewWAL(dataDir + "/wal")
	if err != nil {
		return nil, err
	}

	return &Storage{
		wal:         wal,
		stateFile:   dataDir + "/state",
		snapshotDir: dataDir + "/snapshots",
	}, nil
}

func (s *Storage) SaveTerm(term uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var state raft.PersistentState
	existingFile, err := os.Open(s.stateFile)
	if err == nil {
		dec := gob.NewDecoder(existingFile)
		dec.Decode(&state)
		existingFile.Close()
	}

	logEntries, err := s.wal.ReadAll()
	if err == nil {
		state.Log = logEntries
	}

	state.CurrentTerm = term

	file, err := os.OpenFile(s.stateFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := gob.NewEncoder(file)
	if err := enc.Encode(state); err != nil {
		return err
	}
	return file.Sync()
}

func (s *Storage) SaveVotedFor(votedFor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var state raft.PersistentState
	existingFile, err := os.Open(s.stateFile)
	if err == nil {
		dec := gob.NewDecoder(existingFile)
		dec.Decode(&state)
		existingFile.Close()
	}

	logEntries, err := s.wal.ReadAll()
	if err == nil {
		state.Log = logEntries
	}

	state.VotedFor = votedFor

	file, err := os.OpenFile(s.stateFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := gob.NewEncoder(file)
	if err := enc.Encode(state); err != nil {
		return err
	}
	return file.Sync()
}

func (s *Storage) SaveLog(entries []raft.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.file.Truncate(0); err != nil {
		return err
	}
	if _, err := s.wal.file.Seek(0, 0); err != nil {
		return err
	}

	enc := gob.NewEncoder(s.wal.file)
	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			return err
		}
	}
	return s.wal.file.Sync()
}

func (s *Storage) Load() (*raft.PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return &raft.PersistentState{
				CurrentTerm: 0,
				VotedFor:    "",
				Log:         []raft.LogEntry{},
			}, nil
		}
		return nil, err
	}
	defer file.Close()

	var state raft.PersistentState
	dec := gob.NewDecoder(file)
	if err := dec.Decode(&state); err != nil {
		return nil, err
	}

	logEntries, err := s.wal.ReadAll()
	if err != nil {
		return nil, err
	}
	state.Log = logEntries

	return &state, nil
}

func (s *Storage) SaveSnapshot(data []byte, lastIndex uint64, lastTerm uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshotFile := fmt.Sprintf("%s/snapshot-%d-%d", s.snapshotDir, lastIndex, lastTerm)
	file, err := os.OpenFile(snapshotFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	meta := struct {
		LastIndex uint64
		LastTerm  uint64
		DataLen   uint64
	}{
		LastIndex: lastIndex,
		LastTerm:  lastTerm,
		DataLen:   uint64(len(data)),
	}

	enc := gob.NewEncoder(file)
	if err := enc.Encode(meta); err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		return err
	}
	return file.Sync()
}

func (s *Storage) LoadSnapshot() ([]byte, uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.snapshotDir)
	if err != nil {
		return nil, 0, 0, err
	}

	var latestFile string
	var latestIndex uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			var index, term uint64
			if _, err := fmt.Sscanf(entry.Name(), "snapshot-%d-%d", &index, &term); err == nil {
				if index > latestIndex {
					latestIndex = index
					latestFile = s.snapshotDir + "/" + entry.Name()
				}
			}
		}
	}

	if latestFile == "" {
		return nil, 0, 0, fmt.Errorf("no snapshot found")
	}

	file, err := os.Open(latestFile)
	if err != nil {
		return nil, 0, 0, err
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	var meta struct {
		LastIndex uint64
		LastTerm  uint64
		DataLen   uint64
	}
	if err := dec.Decode(&meta); err != nil {
		return nil, 0, 0, err
	}

	data := make([]byte, meta.DataLen)
	if _, err := file.Read(data); err != nil {
		return nil, 0, 0, err
	}

	return data, meta.LastIndex, meta.LastTerm, nil
}

func (s *Storage) AppendLogEntry(entry raft.LogEntry) error {
	return s.wal.Append(entry)
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}

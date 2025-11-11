package storage

import (
	"encoding/gob"
	"os"
	"sync"

	"github.com/virajbhartiya/raft/pkg/raft"
)

type WAL struct {
	mu       sync.Mutex
	file     *os.File
	filePath string
}

func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:     file,
		filePath: filePath,
	}, nil
}

func (w *WAL) Append(entry raft.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	enc := gob.NewEncoder(w.file)
	if err := enc.Encode(entry); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) ReadAll() ([]raft.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var entries []raft.LogEntry
	dec := gob.NewDecoder(w.file)
	for {
		var entry raft.LogEntry
		if err := dec.Decode(&entry); err != nil {
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (w *WAL) Truncate(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := w.ReadAll()
	if err != nil {
		return err
	}

	var keep []raft.LogEntry
	for _, e := range entries {
		if e.Index <= index {
			keep = append(keep, e)
		}
	}

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	enc := gob.NewEncoder(w.file)
	for _, e := range keep {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

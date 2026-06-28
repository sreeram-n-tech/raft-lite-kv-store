package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
)

type LogEntry struct {
	Index   int64  `json:"index"`
	Term    int64  `json:"term"`
	Command string `json:"command"` // e.g. "PUT:key:value" or "DELETE:key"
}

type Storage struct {
	mu      sync.RWMutex
	kv      map[string]string
	walPath string
	walFile *os.File
	log     []LogEntry // in-memory log of all entries (committed + uncommitted)
	commit  int64      // last committed index
}

func NewStorage(walPath string) (*Storage, error) {
	s := &Storage{
		kv:      make(map[string]string),
		walPath: walPath,
		log:     make([]LogEntry, 0),
		commit:  0,
	}

	// Always ensure there is a dummy entry at index 0 with term 0
	// This simplifies Raft 1-based indexing logic and prevLogIndex checks.
	s.log = append(s.log, LogEntry{Index: 0, Term: 0, Command: ""})

	if err := s.replayWAL(); err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	// Open WAL for append-only writing
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	s.walFile = f

	return s, nil
}

// replayWAL reads the WAL file from disk and reconstructs the log and state machine map.
func (s *Storage) replayWAL() error {
	f, err := os.OpenFile(s.walPath, os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		var entry LogEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			slog.Warn("Skipping corrupt WAL entry", "line", line, "error", err)
			continue
		}

		// Rebuild in-memory log
		// Pad log if indices are non-contiguous or if index is larger than current length
		for int64(len(s.log)) <= entry.Index {
			s.log = append(s.log, LogEntry{})
		}
		s.log[entry.Index] = entry

		// Apply to state machine map
		s.applyToMap(entry.Command)

		if entry.Index > s.commit {
			s.commit = entry.Index
		}
	}

	return scanner.Err()
}

// applyToMap parses the command and applies it to the in-memory map.
func (s *Storage) applyToMap(cmd string) {
	if cmd == "" {
		return
	}
	parts := strings.SplitN(cmd, ":", 3)
	if len(parts) < 2 {
		return
	}
	op := parts[0]
	key := parts[1]

	switch op {
	case "PUT":
		if len(parts) == 3 {
			s.kv[key] = parts[2]
		}
	case "DELETE":
		delete(s.kv, key)
	}
}

// Close closes the WAL file descriptor.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.walFile != nil {
		return s.walFile.Close()
	}
	return nil
}

// AppendAndApply appends a committed log entry to the WAL file, fsyncs it, and applies it to the state machine.
func (s *Storage) AppendAndApply(entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure the log contains this entry in memory
	for int64(len(s.log)) <= entry.Index {
		s.log = append(s.log, LogEntry{})
	}
	s.log[entry.Index] = entry

	// Write to disk WAL
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}

	if _, err := s.walFile.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// fsync after every append
	if err := s.walFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync WAL: %w", err)
	}

	// Apply to in-memory KV map
	s.applyToMap(entry.Command)

	if entry.Index > s.commit {
		s.commit = entry.Index
	}

	return nil
}

// Get returns the value of a key from the state machine.
func (s *Storage) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.kv[key]
	return val, ok
}

// GetLog returns a copy of the in-memory log starting from the given index (inclusive).
func (s *Storage) GetLog(fromIndex int64) []LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if fromIndex < 0 || fromIndex >= int64(len(s.log)) {
		return nil
	}
	copied := make([]LogEntry, len(s.log)-int(fromIndex))
	copy(copied, s.log[fromIndex:])
	return copied
}

// AppendInMemory appends uncommitted entries to the in-memory log (for followers or leader log replication).
// It truncates any conflicting entries at or after the entry's index.
func (s *Storage) AppendInMemory(entries []LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range entries {
		if entry.Index < 1 {
			continue
		}
		// If there is a conflict (same index, different term), truncate the log from this index onwards
		if entry.Index < int64(len(s.log)) {
			if s.log[entry.Index].Term != entry.Term {
				s.log = s.log[:entry.Index]
			} else {
				// Already exists and matches term, skip appending
				continue
			}
		}

		// Extend log if needed
		for int64(len(s.log)) < entry.Index {
			s.log = append(s.log, LogEntry{})
		}
		if int64(len(s.log)) == entry.Index {
			s.log = append(s.log, entry)
		} else {
			s.log[entry.Index] = entry
		}
	}
}

// LastLogInfo returns the index and term of the last entry in the in-memory log.
func (s *Storage) LastLogInfo() (int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.log) == 0 {
		return 0, 0
	}
	last := s.log[len(s.log)-1]
	return last.Index, last.Term
}

// GetEntry returns the entry at the given index from the in-memory log.
func (s *Storage) GetEntry(index int64) (LogEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index < 0 || index >= int64(len(s.log)) {
		return LogEntry{}, false
	}
	return s.log[index], true
}

// CommitIndex returns the last committed index.
func (s *Storage) CommitIndex() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.commit
}

// LogLength returns the total number of entries in the in-memory log.
func (s *Storage) LogLength() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.log))
}

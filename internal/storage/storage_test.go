package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStorageWALReplay(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kvstore-wal-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")

	// 1. Create storage and write some entries
	s, err := NewStorage(walPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	err = s.AppendAndApply(LogEntry{Index: 1, Term: 1, Command: "PUT:foo:bar"})
	if err != nil {
		t.Fatalf("failed to append entry: %v", err)
	}

	err = s.AppendAndApply(LogEntry{Index: 2, Term: 1, Command: "PUT:baz:qux"})
	if err != nil {
		t.Fatalf("failed to append entry: %v", err)
	}

	err = s.AppendAndApply(LogEntry{Index: 3, Term: 2, Command: "DELETE:foo"})
	if err != nil {
		t.Fatalf("failed to append entry: %v", err)
	}

	s.Close()

	// 2. Re-open and verify replay
	s2, err := NewStorage(walPath)
	if err != nil {
		t.Fatalf("failed to re-open storage: %v", err)
	}
	defer s2.Close()

	// Verify key-value map state
	if val, ok := s2.Get("foo"); ok {
		t.Errorf("expected 'foo' to be deleted, got %v", val)
	}

	if val, ok := s2.Get("baz"); !ok || val != "qux" {
		t.Errorf("expected 'baz' to be 'qux', got %v (ok=%v)", val, ok)
	}

	// Verify log state
	lastIdx, lastTerm := s2.LastLogInfo()
	if lastIdx != 3 || lastTerm != 2 {
		t.Errorf("expected last log info to be (3, 2), got (%d, %d)", lastIdx, lastTerm)
	}

	if s2.CommitIndex() != 3 {
		t.Errorf("expected commit index to be 3, got %d", s2.CommitIndex())
	}
}

func TestAppendInMemoryAndConflict(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kvstore-mem-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	s, err := NewStorage(walPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	// Append some uncommitted entries in-memory
	s.AppendInMemory([]LogEntry{
		{Index: 1, Term: 1, Command: "PUT:a:1"},
		{Index: 2, Term: 1, Command: "PUT:b:2"},
		{Index: 3, Term: 1, Command: "PUT:c:3"},
	})

	lastIdx, lastTerm := s.LastLogInfo()
	if lastIdx != 3 || lastTerm != 1 {
		t.Errorf("expected (3, 1), got (%d, %d)", lastIdx, lastTerm)
	}

	// Append entries with conflicts at index 2 (term 2)
	s.AppendInMemory([]LogEntry{
		{Index: 2, Term: 2, Command: "PUT:b:20"},
		{Index: 3, Term: 2, Command: "PUT:c:30"},
		{Index: 4, Term: 2, Command: "PUT:d:40"},
	})

	lastIdx, lastTerm = s.LastLogInfo()
	if lastIdx != 4 || lastTerm != 2 {
		t.Errorf("expected (4, 2) after conflict resolution, got (%d, %d)", lastIdx, lastTerm)
	}

	// Verify entries in memory
	e2, ok := s.GetEntry(2)
	if !ok || e2.Term != 2 || e2.Command != "PUT:b:20" {
		t.Errorf("unexpected entry 2: %+v", e2)
	}
}

func TestStorageConcurrency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kvstore-concurrency-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "concurrency.wal")
	s, err := NewStorage(walPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	const numGoroutines = 10
	const opsPerGoroutine = 100
	var wg sync.WaitGroup

	// Reader goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				_, _ = s.Get(fmt.Sprintf("key-%d", j%10))
				_ = s.GetLog(0)
				_, _ = s.LastLogInfo()
			}
		}(i)
	}

	// Writer goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				idx := int64(id*opsPerGoroutine + j + 1)
				cmd := fmt.Sprintf("PUT:key-%d:value-%d", j%10, idx)
				_ = s.AppendAndApply(LogEntry{Index: idx, Term: 1, Command: cmd})
				s.AppendInMemory([]LogEntry{{Index: idx + 10000, Term: 1, Command: cmd}})
			}
		}(i)
	}

	wg.Wait()
}

func TestCorruptWALReplay(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kvstore-corrupt-wal-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "corrupt.wal")

	// 1. Manually write a WAL file containing 2 valid JSON entries and 1 truncated line at the end
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL file: %v", err)
	}
	
	valid1 := `{"index":1,"term":1,"command":"PUT:a:1"}` + "\n"
	valid2 := `{"index":2,"term":1,"command":"PUT:b:2"}` + "\n"
	corrupt := `{"index":3,"term":2,"command":"PUT:c` // Truncated/corrupt trailing line

	if _, err := f.WriteString(valid1 + valid2 + corrupt); err != nil {
		f.Close()
		t.Fatalf("failed to write WAL: %v", err)
	}
	f.Close()

	// 2. Open storage and verify it starts successfully and ignores the corrupt line
	s, err := NewStorage(walPath)
	if err != nil {
		t.Fatalf("NewStorage failed on corrupt WAL: %v", err)
	}
	defer s.Close()

	// Verify valid entries are recovered
	valA, okA := s.Get("a")
	if !okA || valA != "1" {
		t.Errorf("expected 'a' to be '1', got %s (ok=%v)", valA, okA)
	}

	valB, okB := s.Get("b")
	if !okB || valB != "2" {
		t.Errorf("expected 'b' to be '2', got %s (ok=%v)", valB, okB)
	}

	// Verify the corrupt entry 'c' was NOT applied
	valC, okC := s.Get("c")
	if okC {
		t.Errorf("expected 'c' to be absent, but got %s", valC)
	}

	// Verify log size and commit index reflect only the valid entries
	lastIdx, _ := s.LastLogInfo()
	if lastIdx != 2 {
		t.Errorf("expected last log index to be 2, got %d", lastIdx)
	}

	if s.CommitIndex() != 2 {
		t.Errorf("expected commit index to be 2, got %d", s.CommitIndex())
	}
}


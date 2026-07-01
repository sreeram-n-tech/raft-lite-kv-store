import os
import json
import logging
import threading
from typing import List, Tuple, Dict, Optional

class LogEntry:
    def __init__(self, index: int = 0, term: int = 0, command: str = ""):
        self.index = index
        self.term = term
        self.command = command

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "term": self.term,
            "command": self.command,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'LogEntry':
        return cls(
            index=d.get("index", 0),
            term=d.get("term", 0),
            command=d.get("command", ""),
        )

    def __repr__(self) -> str:
        return f"LogEntry(index={self.index}, term={self.term}, command='{self.command}')"


class Storage:
    def __init__(self, wal_path: str):
        self.mu = threading.RLock()
        self.kv: Dict[str, str] = {}
        self.wal_path = wal_path
        self.log: List[LogEntry] = []
        self.commit: int = 0

        # Always ensure there is a dummy entry at index 0 with term 0
        self.log.append(LogEntry(index=0, term=0, command=""))

        # Replay WAL to restore state
        self._replay_wal()

        # Open WAL for append-only writing
        self.wal_file = open(wal_path, "a", encoding="utf-8")

    def _replay_wal(self):
        if not os.path.exists(self.wal_path):
            return

        with open(self.wal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    entry = LogEntry.from_dict(data)
                except Exception as e:
                    logging.warning("Skipping corrupt WAL entry: %s, error: %s", line, e)
                    continue

                # Rebuild in-memory log
                while len(self.log) <= entry.index:
                    self.log.append(LogEntry())
                self.log[entry.index] = entry

                # Apply to state machine map
                self._apply_to_map(entry.command)

                if entry.index > self.commit:
                    self.commit = entry.index

    def _apply_to_map(self, cmd: str):
        if not cmd:
            return
        parts = cmd.split(":", 2)
        if len(parts) < 2:
            return
        op = parts[0]
        key = parts[1]

        if op == "PUT":
            if len(parts) == 3:
                self.kv[key] = parts[2]
        elif op == "DELETE":
            self.kv.pop(key, None)

    def close(self):
        with self.mu:
            if hasattr(self, 'wal_file') and self.wal_file:
                self.wal_file.close()

    def append_and_apply(self, entry: LogEntry) -> None:
        with self.mu:
            # Ensure the log contains this entry in memory
            while len(self.log) <= entry.index:
                self.log.append(LogEntry())
            self.log[entry.index] = entry

            # Write to disk WAL
            line = json.dumps(entry.to_dict(), separators=(',', ':')) + "\n"
            self.wal_file.write(line)
            self.wal_file.flush()
            try:
                os.fsync(self.wal_file.fileno())
            except Exception as e:
                # Some environments might not support fsync on certain file descriptors
                pass

            # Apply to in-memory KV map
            self._apply_to_map(entry.command)

            if entry.index > self.commit:
                self.commit = entry.index

    def get(self, key: str) -> Tuple[str, bool]:
        with self.mu:
            if key in self.kv:
                return self.kv[key], True
            return "", False

    def get_log(self, from_index: int) -> List[LogEntry]:
        with self.mu:
            if from_index < 0 or from_index >= len(self.log):
                return []
            # Return a copy to avoid concurrency/modification issues
            return [LogEntry(e.index, e.term, e.command) for e in self.log[from_index:]]

    def append_in_memory(self, entries: List[LogEntry]) -> None:
        with self.mu:
            for entry in entries:
                if entry.index < 1:
                    continue
                # If there is a conflict (same index, different term), truncate the log from this index onwards
                if entry.index < len(self.log):
                    if self.log[entry.index].term != entry.term:
                        self.log = self.log[:entry.index]
                    else:
                        # Already exists and matches term, skip appending
                        continue

                # Extend log if needed
                while len(self.log) < entry.index:
                    self.log.append(LogEntry())
                if len(self.log) == entry.index:
                    self.log.append(entry)
                else:
                    self.log[entry.index] = entry

    def last_log_info(self) -> Tuple[int, int]:
        with self.mu:
            if not self.log:
                return 0, 0
            last = self.log[-1]
            return last.index, last.term

    def get_entry(self, index: int) -> Tuple[Optional[LogEntry], bool]:
        with self.mu:
            if index < 0 or index >= len(self.log):
                return None, False
            return self.log[index], True

    def commit_index(self) -> int:
        with self.mu:
            return self.commit

    def log_length(self) -> int:
        with self.mu:
            return len(self.log)

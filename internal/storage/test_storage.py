import os
import tempfile
import threading
import unittest
from internal.storage.storage import Storage, LogEntry

class TestStorage(unittest.TestCase):
    def test_wal_replay(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            wal_path = os.path.join(tmp_dir, "test.wal")

            # 1. Create storage and write some entries
            s = Storage(wal_path)
            s.append_and_apply(LogEntry(index=1, term=1, command="PUT:foo:bar"))
            s.append_and_apply(LogEntry(index=2, term=1, command="PUT:baz:qux"))
            s.append_and_apply(LogEntry(index=3, term=2, command="DELETE:foo"))
            s.close()

            # 2. Re-open and verify replay
            s2 = Storage(wal_path)
            try:
                val, ok = s2.get("foo")
                self.assertFalse(ok, f"expected 'foo' to be deleted, got {val}")

                val, ok = s2.get("baz")
                self.assertTrue(ok)
                self.assertEqual(val, "qux")

                last_idx, last_term = s2.last_log_info()
                self.assertEqual(last_idx, 3)
                self.assertEqual(last_term, 2)

                self.assertEqual(s2.commit_index(), 3)
            finally:
                s2.close()

    def test_append_in_memory_and_conflict(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            wal_path = os.path.join(tmp_dir, "test.wal")
            s = Storage(wal_path)
            try:
                s.append_in_memory([
                    LogEntry(index=1, term=1, command="PUT:a:1"),
                    LogEntry(index=2, term=1, command="PUT:b:2"),
                    LogEntry(index=3, term=1, command="PUT:c:3"),
                ])

                last_idx, last_term = s.last_log_info()
                self.assertEqual(last_idx, 3)
                self.assertEqual(last_term, 1)

                # Append entries with conflicts at index 2 (term 2)
                s.append_in_memory([
                    LogEntry(index=2, term=2, command="PUT:b:20"),
                    LogEntry(index=3, term=2, command="PUT:c:30"),
                    LogEntry(index=4, term=2, command="PUT:d:40"),
                ])

                last_idx, last_term = s.last_log_info()
                self.assertEqual(last_idx, 4)
                self.assertEqual(last_term, 2)

                entry, ok = s.get_entry(2)
                self.assertTrue(ok)
                self.assertEqual(entry.term, 2)
                self.assertEqual(entry.command, "PUT:b:20")
            finally:
                s.close()

    def test_storage_concurrency(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            wal_path = os.path.join(tmp_dir, "concurrency.wal")
            s = Storage(wal_path)
            try:
                num_threads = 10
                ops_per_thread = 100
                threads = []

                # Reader thread target
                def reader():
                    for j in range(ops_per_thread):
                        s.get(f"key-{j%10}")
                        s.get_log(0)
                        s.last_log_info()

                # Writer thread target
                def writer(thread_id):
                    for j in range(ops_per_thread):
                        idx = thread_id * ops_per_thread + j + 1
                        cmd = f"PUT:key-{j%10}:value-{idx}"
                        s.append_and_apply(LogEntry(index=idx, term=1, command=cmd))
                        s.append_in_memory([LogEntry(index=idx + 10000, term=1, command=cmd)])

                # Spawn readers and writers
                for i in range(num_threads):
                    t_read = threading.Thread(target=reader)
                    t_write = threading.Thread(target=writer, args=(i,))
                    threads.append(t_read)
                    threads.append(t_write)
                    t_read.start()
                    t_write.start()

                for t in threads:
                    t.join()
            finally:
                s.close()

    def test_corrupt_wal_replay(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            wal_path = os.path.join(tmp_dir, "corrupt.wal")

            # 1. Manually write a WAL file containing 2 valid JSON entries and 1 truncated line at the end
            with open(wal_path, "w", encoding="utf-8") as f:
                f.write('{"index":1,"term":1,"command":"PUT:a:1"}\n')
                f.write('{"index":2,"term":1,"command":"PUT:b:2"}\n')
                f.write('{"index":3,"term":2,"command":"PUT:c')  # Truncated trailing line

            # 2. Open storage and verify it starts successfully and ignores the corrupt line
            s = Storage(wal_path)
            try:
                val_a, ok_a = s.get("a")
                self.assertTrue(ok_a)
                self.assertEqual(val_a, "1")

                val_b, ok_b = s.get("b")
                self.assertTrue(ok_b)
                self.assertEqual(val_b, "2")

                val_c, ok_c = s.get("c")
                self.assertFalse(ok_c)

                last_idx, _ = s.last_log_info()
                self.assertEqual(last_idx, 2)
                self.assertEqual(s.commit_index(), 2)
            finally:
                s.close()

if __name__ == "__main__":
    unittest.main()

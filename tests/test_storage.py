"""
ZenithDB Storage Core Unit Tests
Tests Write-Ahead Log (WAL), Bloom Filter, SSTable binary I/O, and LSM-Tree lifecycle.
"""

import os
import shutil
import tempfile
import unittest

from zenith.storage.bloom import BloomFilter
from zenith.storage.lsm import LSMTree
from zenith.storage.sstable import SSTableReader, SSTableWriter, TOMBSTONE
from zenith.storage.wal import WALFrame, WALOpType, WriteAheadLog


class TestBloomFilter(unittest.TestCase):
    def test_membership_and_false_positives(self):
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        keys = [f"key_{i}".encode("utf-8") for i in range(500)]

        for k in keys:
            bf.add(k)

        # True positives
        for k in keys:
            self.assertTrue(bf.contains(k))

        # False positive rate test
        missing = [f"missing_{i}".encode("utf-8") for i in range(1000)]
        fp_count = sum(1 for m in missing if bf.contains(m))
        fp_rate = fp_count / len(missing)
        self.assertLess(fp_rate, 0.05, "False positive rate exceeds threshold")

    def test_serialization_roundtrip(self):
        bf = BloomFilter(capacity=500, error_rate=0.01)
        for i in range(200):
            bf.add(f"item_{i}".encode("utf-8"))

        data = bf.to_bytes()
        restored = BloomFilter.from_bytes(data)

        for i in range(200):
            self.assertTrue(restored.contains(f"item_{i}".encode("utf-8")))
        self.assertFalse(restored.contains(b"non_existent_key_9999"))


class TestWriteAheadLog(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_wal_append_and_recovery(self):
        wal = WriteAheadLog(self.test_dir, sync_mode="always")
        lsn1 = wal.append(WALOpType.SET, b"user:100", b'{"name": "Alice"}')
        lsn2 = wal.append(WALOpType.SET, b"user:101", b'{"name": "Bob"}')
        lsn3 = wal.append(WALOpType.DEL, b"user:100", b"")
        wal.close()

        # Recover from disk
        wal_recovered = WriteAheadLog(self.test_dir)
        frames = wal_recovered.recover()
        self.assertEqual(len(frames), 3)
        self.assertEqual(frames[0].lsn, lsn1)
        self.assertEqual(frames[0].key, b"user:100")
        self.assertEqual(frames[1].key, b"user:101")
        self.assertEqual(frames[2].op_type, WALOpType.DEL)
        wal_recovered.close()

    def test_wal_crash_truncation_resilience(self):
        """Simulates sudden power-cut leaving half-written trailing bytes in WAL."""
        wal = WriteAheadLog(self.test_dir, sync_mode="always")
        wal.append(WALOpType.SET, b"k1", b"v1")
        wal.append(WALOpType.SET, b"k2", b"v2")
        wal.close()

        # Corrupt file by appending partial garbage bytes
        log_file = os.path.join(self.test_dir, os.listdir(self.test_dir)[0])
        with open(log_file, "ab") as f:
            f.write(b"CORRUPTED_TRAILING_PARTIAL_BYTES_WITHOUT_VALID_FRAME")

        wal_recovered = WriteAheadLog(self.test_dir)
        frames = wal_recovered.recover()
        # Should cleanly recover the 2 valid frames and ignore the trailing corruption
        self.assertEqual(len(frames), 2)
        self.assertEqual(frames[0].key, b"k1")
        self.assertEqual(frames[1].key, b"k2")
        wal_recovered.close()


class TestSSTable(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.sst_path = os.path.join(self.test_dir, "test.sst")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_sstable_write_and_binary_search(self):
        writer = SSTableWriter(self.sst_path, index_interval=4)
        pairs = [(f"key_{i:04d}".encode("utf-8"), f"val_{i:04d}".encode("utf-8")) for i in range(100)]
        for k, v in pairs:
            writer.write_entry(k, v)
        writer.finish()

        reader = SSTableReader(self.sst_path)
        self.assertEqual(reader.entry_count, 100)

        # Point lookups
        for k, v in pairs:
            self.assertEqual(reader.get(k), v)

        # Missing keys
        self.assertIsNone(reader.get(b"key_9999"))
        self.assertIsNone(reader.get(b"aaaa_before_min"))
        self.assertIsNone(reader.get(b"zzzz_after_max"))

        # Range scan
        scanned = list(reader.scan(b"key_0020", b"key_0030"))
        self.assertEqual(len(scanned), 11)
        self.assertEqual(scanned[0][0], b"key_0020")
        self.assertEqual(scanned[-1][0], b"key_0030")

        reader.close()


class TestLSMTree(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_lsm_flush_and_compaction(self):
        # Use small 1KB memtable to force multiple flushes
        lsm = LSMTree(self.test_dir, memtable_size_bytes=512)

        for i in range(100):
            lsm.put(f"key_{i:03d}".encode("utf-8"), f"val_{i:03d}".encode("utf-8"))

        lsm.flush()
        # Verify all keys retrievable from SSTables
        for i in range(100):
            self.assertEqual(
                lsm.get(f"key_{i:03d}".encode("utf-8")),
                f"val_{i:03d}".encode("utf-8"),
            )

        # Delete some keys
        for i in range(0, 50):
            lsm.delete(f"key_{i:03d}".encode("utf-8"))

        # Compact SSTables
        lsm.compact()

        # Verify deleted are None and remaining exist
        for i in range(0, 50):
            self.assertIsNone(lsm.get(f"key_{i:03d}".encode("utf-8")))
        for i in range(50, 100):
            self.assertEqual(
                lsm.get(f"key_{i:03d}".encode("utf-8")),
                f"val_{i:03d}".encode("utf-8"),
            )

        lsm.close()


if __name__ == "__main__":
    unittest.main()

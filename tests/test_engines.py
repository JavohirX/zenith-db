"""
ZenithDB Engine Layer Unit Tests
Tests KV data structures, TTL, Document Store JSONPath, BM25 text relevance, Vector metrics, and ACID Transactions.
"""

import shutil
import tempfile
import time
import unittest

from zenith.engine.doc import DocumentStore
from zenith.engine.kv import KeyValueEngine
from zenith.engine.text import FullTextIndex, PorterStemmer
from zenith.engine.txn import TransactionManager
from zenith.engine.vector import (
    VectorIndex,
    cosine_similarity,
    euclidean_distance,
    vector_dot,
)
from zenith.storage.lsm import LSMTree


class TestKeyValueEngine(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.lsm = LSMTree(self.test_dir)
        self.kv = KeyValueEngine(self.lsm)

    def tearDown(self):
        self.lsm.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_strings_and_increments(self):
        self.assertTrue(self.kv.set("counter", 10))
        self.assertEqual(self.kv.incrby("counter", 5), 15)
        self.assertEqual(self.kv.decrby("counter", 2), 13)
        self.assertEqual(self.kv.get("counter"), "13")
        self.assertEqual(self.kv.strlen("counter"), 2)
        self.assertEqual(self.kv.append("counter", "_appended"), 11)
        self.assertEqual(self.kv.get("counter"), "13_appended")

        # SETNX
        self.assertFalse(self.kv.setnx("counter", "new_val"))
        self.assertTrue(self.kv.setnx("new_key", "created"))
        self.assertEqual(self.kv.get("new_key"), "created")

    def test_hashes(self):
        self.kv.hset("user:1", "name", "Grace")
        self.kv.hset("user:1", "role", "Engineer")
        self.assertEqual(self.kv.hget("user:1", "name"), "Grace")
        self.assertEqual(self.kv.hlen("user:1"), 2)
        self.assertEqual(self.kv.hgetall("user:1"), {"name": "Grace", "role": "Engineer"})
        self.assertEqual(self.kv.hincrby("user:1", "logins", 1), 1)
        self.assertEqual(self.kv.hincrby("user:1", "logins", 5), 6)
        self.kv.hdel("user:1", "role")
        self.assertFalse(self.kv.hexists("user:1", "role"))

    def test_lists(self):
        self.kv.rpush("tasks", "task1", "task2", "task3", "task4")
        self.assertEqual(self.kv.llen("tasks"), 4)
        self.assertEqual(self.kv.lrange("tasks", 0, -1), ["task1", "task2", "task3", "task4"])
        self.assertEqual(self.kv.lindex("tasks", 1), "task2")
        self.assertTrue(self.kv.lset("tasks", 1, "task2_updated"))
        self.assertEqual(self.kv.lindex("tasks", 1), "task2_updated")
        self.kv.ltrim("tasks", 1, 2)
        self.assertEqual(self.kv.lrange("tasks", 0, -1), ["task2_updated", "task3"])
        self.assertEqual(self.kv.lpop("tasks"), "task2_updated")
        self.assertEqual(self.kv.rpop("tasks"), "task3")
        self.assertEqual(self.kv.lrange("tasks", 0, -1), [])

    def test_sets(self):
        self.kv.sadd("set_a", "1", "2", "3")
        self.kv.sadd("set_b", "2", "3", "4")
        self.assertEqual(self.kv.scard("set_a"), 3)
        self.assertTrue(self.kv.sismember("set_a", "1"))
        self.assertFalse(self.kv.sismember("set_a", "99"))

        # Set operations: Union, Inter, Diff
        self.assertEqual(self.kv.sunion("set_a", "set_b"), {"1", "2", "3", "4"})
        self.assertEqual(self.kv.sinter("set_a", "set_b"), {"2", "3"})
        self.assertEqual(self.kv.sdiff("set_a", "set_b"), {"1"})

    def test_sorted_sets(self):
        self.kv.zadd("leaderboard", {"player1": 100.0, "player2": 250.5, "player3": 50.0})
        self.assertEqual(self.kv.zscore("leaderboard", "player2"), 250.5)
        # Zincrby & Zrank
        self.assertEqual(self.kv.zincrby("leaderboard", 20.0, "player3"), 70.0)
        self.assertEqual(self.kv.zrank("leaderboard", "player3"), 0)
        self.assertEqual(self.kv.zrank("leaderboard", "player2"), 2)

        # Range by ascending score
        ranked = self.kv.zrange("leaderboard", 0, -1)
        self.assertEqual(ranked, ["player3", "player1", "player2"])

    def test_ttl_expiration(self):
        self.kv.set("ephemeral", "hello", ex=1)
        self.assertEqual(self.kv.get("ephemeral"), "hello")
        self.assertGreaterEqual(self.kv.ttl("ephemeral"), 0)
        time.sleep(1.2)
        self.assertIsNone(self.kv.get("ephemeral"))
        self.assertEqual(self.kv.ttl("ephemeral"), -2)


class TestDocumentStore(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.lsm = LSMTree(self.test_dir)
        self.docs = DocumentStore(self.lsm)

    def tearDown(self):
        self.lsm.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_document_crud_and_queries(self):
        self.docs.insert("users", "u1", {"name": "Alice", "age": 30, "status": "active", "tags": ["admin", "dev"]})
        self.docs.insert("users", "u2", {"name": "Bob", "age": 20, "status": "pending", "tags": ["dev"]})
        self.docs.insert("users", "u3", {"name": "Charlie", "age": 45, "status": "active", "tags": ["lead"]})

        # Point get
        doc1 = self.docs.get("users", "u1")
        self.assertEqual(doc1["name"], "Alice")

        # Query filter: status == "active" and age >= 30
        res = self.docs.query("users", filter_dict={"status": "active", "age": {"$gte": 30}}, sort_by="age")
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]["_id"], "u1")
        self.assertEqual(res[1]["_id"], "u3")

        # Query contains
        res_devs = self.docs.query("users", filter_dict={"tags": {"$contains": "dev"}})
        self.assertEqual(len(res_devs), 2)

        # Delete document
        self.assertTrue(self.docs.delete("users", "u2"))
        self.assertIsNone(self.docs.get("users", "u2"))
        self.assertEqual(self.docs.count("users"), 2)


class TestFullTextIndex(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.lsm = LSMTree(self.test_dir)
        self.ft = FullTextIndex(self.lsm, "articles")

    def tearDown(self):
        self.lsm.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_porter_stemmer(self):
        self.assertEqual(PorterStemmer.stem("connecting"), "connect")
        self.assertEqual(PorterStemmer.stem("relational"), "relate")
        self.assertEqual(PorterStemmer.stem("databases"), "database")

    def test_bm25_search_relevance(self):
        self.ft.index_document("doc1", "Relational databases provide ACID guarantees and structured tables.")
        self.ft.index_document("doc2", "Vector databases index high dimensional embeddings for similarity search.")
        self.ft.index_document("doc3", "LSM Tree storage engines provide fast append writes and compaction.")

        results = self.ft.search("ACID relational databases")
        self.assertTrue(len(results) > 0)
        self.assertEqual(results[0]["doc_id"], "doc1")
        self.assertIn("<b>relational</b>", results[0]["snippet"].lower())

        results_vec = self.ft.search("vector embeddings similarity")
        self.assertEqual(results_vec[0]["doc_id"], "doc2")


class TestVectorIndex(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.lsm = LSMTree(self.test_dir)
        self.vidx = VectorIndex(self.lsm, "embeddings", dimension=3)

    def tearDown(self):
        self.lsm.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_vector_math_and_search(self):
        v1 = [1.0, 0.0, 0.0]
        v2 = [0.0, 1.0, 0.0]
        v3 = [0.99, 0.01, 0.0]

        self.assertAlmostEqual(cosine_similarity(v1, v1), 1.0)
        self.assertAlmostEqual(cosine_similarity(v1, v2), 0.0)
        self.assertAlmostEqual(euclidean_distance(v1, v1), 0.0)

        self.vidx.insert("x_axis", v1, {"axis": "x"})
        self.vidx.insert("y_axis", v2, {"axis": "y"})
        self.vidx.insert("near_x", v3, {"axis": "x_close"})

        # Search nearest to v1
        results = self.vidx.search([1.0, 0.0, 0.0], top_k=2, metric="cosine")
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["vector_id"], "x_axis")
        self.assertEqual(results[1]["vector_id"], "near_x")


class TestTransactions(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.lsm = LSMTree(self.test_dir)
        self.txn_mgr = TransactionManager(self.lsm)

    def tearDown(self):
        self.lsm.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_transaction_commit_and_rollback(self):
        with self.txn_mgr.begin() as tx:
            tx.set("account:A", "100")
            tx.set("account:B", "200")
            self.assertEqual(tx.get("account:A"), "100")

        # Verified committed
        self.assertEqual(self.lsm.get(b"__V__:account:A"), b"100")
        self.assertEqual(self.lsm.get(b"__V__:account:B"), b"200")
        self.assertEqual(self.lsm.get(b"__TYPE__:account:A"), b"string")

        # Rollback on exception
        try:
            with self.txn_mgr.begin() as tx:
                tx.set("account:A", "50")
                raise ValueError("Simulated failure inside transaction")
        except ValueError:
            pass

        # Value should remain untouched at 100
        self.assertEqual(self.lsm.get(b"__V__:account:A"), b"100")


if __name__ == "__main__":
    unittest.main()

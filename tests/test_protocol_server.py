"""
ZenithDB Network & Protocol Unit Tests
Tests RESP streaming parser, RESP serializer, and command execution logic.
"""

import json
import shutil
import tempfile
import unittest

from zenith.protocol.resp import RESPParser, RESPSerializer
from zenith.server.tcp import TCPServer
from zenith.storage.lsm import LSMTree


class TestRESPProtocol(unittest.TestCase):
    def test_serializer(self):
        self.assertEqual(RESPSerializer.ok(), b"+OK\r\n")
        self.assertEqual(RESPSerializer.integer(42), b":42\r\n")
        self.assertEqual(RESPSerializer.bulk_string("hello"), b"$5\r\nhello\r\n")
        self.assertEqual(RESPSerializer.bulk_string(None), b"$-1\r\n")
        self.assertEqual(
            RESPSerializer.array(["a", "b"]),
            b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        )

    def test_parser_single_packet(self):
        parser = RESPParser()
        parser.feed(b"*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n")
        msg = parser.get_next()
        self.assertEqual(msg, ["GET", "user"])

    def test_parser_fragmented_packet(self):
        """Simulates TCP packet fragmentation across multiple reads."""
        parser = RESPParser()
        parser.feed(b"*3\r\n$3\r\nSET\r\n$")
        self.assertIsNone(parser.get_next())

        parser.feed(b"3\r\nfoo\r\n$3\r\n")
        self.assertIsNone(parser.get_next())

        parser.feed(b"bar\r\n")
        msg = parser.get_next()
        self.assertEqual(msg, ["SET", "foo", "bar"])


class TestServerCommandExecution(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.lsm = LSMTree(self.test_dir)
        self.server = TCPServer(self.lsm)

    def tearDown(self):
        self.lsm.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_ping_and_echo(self):
        self.assertEqual(self.server.execute_command(["PING"]), b"+PONG\r\n")
        self.assertEqual(self.server.execute_command(["ECHO", "test"]), b"$4\r\ntest\r\n")

    def test_set_get_del_flow(self):
        resp = self.server.execute_command(["SET", "msg", "world"])
        self.assertEqual(resp, b"+OK\r\n")

        resp = self.server.execute_command(["GET", "msg"])
        self.assertEqual(resp, b"$5\r\nworld\r\n")

        resp = self.server.execute_command(["DEL", "msg"])
        self.assertEqual(resp, b":1\r\n")

        resp = self.server.execute_command(["GET", "msg"])
        self.assertEqual(resp, b"$-1\r\n")

    def test_extended_redis_commands(self):
        # SETNX
        self.assertEqual(self.server.execute_command(["SETNX", "unique_k", "val1"]), b":1\r\n")
        self.assertEqual(self.server.execute_command(["SETNX", "unique_k", "val2"]), b":0\r\n")

        # APPEND & STRLEN
        self.assertEqual(self.server.execute_command(["APPEND", "unique_k", "_more"]), b":9\r\n")
        self.assertEqual(self.server.execute_command(["STRLEN", "unique_k"]), b":9\r\n")

        # HINCRBY
        self.assertEqual(self.server.execute_command(["HINCRBY", "user_h", "score", "5"]), b":5\r\n")

        # LPUSH, LINDEX, LTRIM
        self.server.execute_command(["RPUSH", "queue", "a", "b", "c", "d"])
        self.assertEqual(self.server.execute_command(["LINDEX", "queue", "2"]), b"$1\r\nc\r\n")
        self.assertEqual(self.server.execute_command(["LTRIM", "queue", "1", "2"]), b"+OK\r\n")
        self.assertEqual(self.server.execute_command(["LLEN", "queue"]), b":2\r\n")

    def test_document_and_search_commands(self):
        # Document insert
        insert_res = self.server.execute_command(["DOC.INSERT", "items", "item1", json.dumps({"title": "Book", "price": 25})])
        self.assertIn(b"item1", insert_res)

        # Document get
        get_res = self.server.execute_command(["DOC.GET", "items", "item1"])
        self.assertIn(b"Book", get_res)

        # Document query
        query_res = self.server.execute_command(["DOC.QUERY", "items", json.dumps({"price": {"$gte": 20}})])
        self.assertIn(b"item1", query_res)

        # Document delete
        del_res = self.server.execute_command(["DOC.DELETE", "items", "item1"])
        self.assertEqual(del_res, b":1\r\n")


if __name__ == "__main__":
    unittest.main()

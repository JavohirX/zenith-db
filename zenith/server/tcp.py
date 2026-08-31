"""
ZenithDB Asyncio RESP TCP Server
Compatible with standard Redis clients (redis-cli, redis-py, ioredis, go-redis).
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

from zenith.engine.doc import DocumentStore
from zenith.engine.kv import KeyValueEngine
from zenith.engine.text import FullTextIndex
from zenith.engine.vector import VectorIndex
from zenith.protocol.resp import RESPParser, RESPSerializer
from zenith.storage.lsm import LSMTree

logger = logging.getLogger("zenith.tcp")


class TCPServer:
    """Asyncio TCP server processing RESP commands."""

    def __init__(
        self,
        lsm: LSMTree,
        host: str = "127.0.0.1",
        port: int = 6379,
    ) -> None:
        self.lsm = lsm
        self.host = host
        self.port = port
        self.kv = KeyValueEngine(lsm)
        self.doc_store = DocumentStore(lsm)
        self.text_indexes: Dict[str, FullTextIndex] = {}
        self.vector_indexes: Dict[str, VectorIndex] = {}
        self.start_time = time.time()
        self.total_ops = 0
        self._server: Optional[asyncio.AbstractServer] = None

    def _get_text_index(self, namespace: str) -> FullTextIndex:
        if namespace not in self.text_indexes:
            self.text_indexes[namespace] = FullTextIndex(self.lsm, namespace)
        return self.text_indexes[namespace]

    def _get_vector_index(self, namespace: str) -> VectorIndex:
        if namespace not in self.vector_indexes:
            self.vector_indexes[namespace] = VectorIndex(self.lsm, namespace)
        return self.vector_indexes[namespace]

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handles an individual TCP client connection."""
        parser = RESPParser()
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break

                parser.feed(data)
                while True:
                    cmd_obj = parser.get_next()
                    if cmd_obj is None:
                        break

                    resp_bytes = self.execute_command(cmd_obj)
                    writer.write(resp_bytes)
                    await writer.drain()
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def execute_command(self, cmd_obj: Any) -> bytes:
        """Executes a parsed command and returns RESP binary response."""
        self.total_ops += 1
        if not cmd_obj:
            return RESPSerializer.error("empty command")

        if isinstance(cmd_obj, list):
            parts = [str(p) for p in cmd_obj]
        elif isinstance(cmd_obj, str):
            parts = cmd_obj.split()
        else:
            return RESPSerializer.error("invalid command format")

        if not parts:
            return RESPSerializer.error("empty command")

        cmd = parts[0].upper()
        args = parts[1:]

        try:
            # 1. Connection & Server Info
            if cmd == "PING":
                msg = args[0] if args else "PONG"
                return RESPSerializer.simple_string(msg)

            elif cmd == "ECHO":
                return RESPSerializer.bulk_string(args[0] if args else "")

            elif cmd == "COMMAND":
                return RESPSerializer.array([])

            elif cmd == "INFO":
                uptime = int(time.time() - self.start_time)
                info_text = (
                    f"# Server\r\n"
                    f"zenith_version:1.0.0\r\n"
                    f"os:windows\r\n"
                    f"uptime_in_seconds:{uptime}\r\n"
                    f"total_ops_processed:{self.total_ops}\r\n"
                    f"dbsize:{self.kv.dbsize()}\r\n"
                )
                return RESPSerializer.bulk_string(info_text)

            elif cmd == "DBSIZE":
                return RESPSerializer.integer(self.kv.dbsize())

            elif cmd == "FLUSHDB" or cmd == "FLUSHALL":
                self.kv.flushdb()
                return RESPSerializer.ok()

            elif cmd == "COMPACT":
                self.lsm.compact()
                return RESPSerializer.ok()

            # 2. String Commands
            elif cmd == "GET":
                if not args:
                    return RESPSerializer.error("wrong number of arguments for 'get'")
                val = self.kv.get(args[0])
                return RESPSerializer.bulk_string(val)

            elif cmd == "SET":
                if len(args) < 2:
                    return RESPSerializer.error("wrong number of arguments for 'set'")
                key, val = args[0], args[1]
                ex = None
                nx, xx = False, False

                idx = 2
                while idx < len(args):
                    flag = args[idx].upper()
                    if flag == "EX" and idx + 1 < len(args):
                        ex = int(args[idx + 1])
                        idx += 2
                    elif flag == "NX":
                        nx = True
                        idx += 1
                    elif flag == "XX":
                        xx = True
                        idx += 1
                    else:
                        idx += 1

                res = self.kv.set(key, val, ex=ex, nx=nx, xx=xx)
                return RESPSerializer.ok() if res else RESPSerializer.bulk_string(None)

            elif cmd == "SETNX":
                if len(args) < 2:
                    return RESPSerializer.error("wrong number of arguments for 'setnx'")
                res = self.kv.setnx(args[0], args[1])
                return RESPSerializer.integer(1 if res else 0)

            elif cmd == "SETEX":
                if len(args) < 3:
                    return RESPSerializer.error("wrong number of arguments for 'setex'")
                self.kv.set(args[0], args[2], ex=int(args[1]))
                return RESPSerializer.ok()

            elif cmd == "MGET":
                vals = self.kv.mget(args)
                return RESPSerializer.array(vals)

            elif cmd == "MSET":
                if len(args) % 2 != 0:
                    return RESPSerializer.error("wrong number of arguments for 'mset'")
                mapping = {args[i]: args[i + 1] for i in range(0, len(args), 2)}
                self.kv.mset(mapping)
                return RESPSerializer.ok()

            elif cmd == "INCR":
                new_val = self.kv.incrby(args[0], 1)
                return RESPSerializer.integer(new_val)

            elif cmd == "INCRBY":
                new_val = self.kv.incrby(args[0], int(args[1]))
                return RESPSerializer.integer(new_val)

            elif cmd == "DECR":
                new_val = self.kv.decrby(args[0], 1)
                return RESPSerializer.integer(new_val)

            elif cmd == "DECRBY":
                new_val = self.kv.decrby(args[0], int(args[1]))
                return RESPSerializer.integer(new_val)

            elif cmd == "APPEND":
                if len(args) < 2:
                    return RESPSerializer.error("wrong number of arguments for 'append'")
                new_len = self.kv.append(args[0], args[1])
                return RESPSerializer.integer(new_len)

            elif cmd == "STRLEN":
                if not args:
                    return RESPSerializer.error("wrong number of arguments for 'strlen'")
                return RESPSerializer.integer(self.kv.strlen(args[0]))

            # 3. Generic Key Management
            elif cmd == "DEL":
                count = self.kv.delete(*args)
                return RESPSerializer.integer(count)

            elif cmd == "EXISTS":
                count = self.kv.exists(*args)
                return RESPSerializer.integer(count)

            elif cmd == "EXPIRE":
                res = self.kv.expire(args[0], int(args[1]))
                return RESPSerializer.integer(1 if res else 0)

            elif cmd == "TTL":
                return RESPSerializer.integer(self.kv.ttl(args[0]))

            elif cmd == "PERSIST":
                res = self.kv.persist(args[0])
                return RESPSerializer.integer(1 if res else 0)

            elif cmd == "TYPE":
                return RESPSerializer.simple_string(self.kv.type(args[0]))

            elif cmd == "KEYS":
                pattern = args[0] if args else "*"
                return RESPSerializer.array(self.kv.keys(pattern))

            # 4. Hash Commands
            elif cmd == "HSET":
                key = args[0]
                if (len(args) - 1) % 2 != 0:
                    return RESPSerializer.error("wrong number of arguments for 'hset'")
                added = 0
                for i in range(1, len(args), 2):
                    added += self.kv.hset(key, args[i], args[i + 1])
                return RESPSerializer.integer(added)

            elif cmd == "HGET":
                val = self.kv.hget(args[0], args[1])
                return RESPSerializer.bulk_string(val)

            elif cmd == "HDEL":
                count = self.kv.hdel(args[0], *args[1:])
                return RESPSerializer.integer(count)

            elif cmd == "HGETALL":
                data = self.kv.hgetall(args[0])
                return RESPSerializer.array(data)

            elif cmd == "HKEYS":
                return RESPSerializer.array(self.kv.hkeys(args[0]))

            elif cmd == "HVALS":
                return RESPSerializer.array(self.kv.hvals(args[0]))

            elif cmd == "HEXISTS":
                res = self.kv.hexists(args[0], args[1])
                return RESPSerializer.integer(1 if res else 0)

            elif cmd == "HLEN":
                return RESPSerializer.integer(self.kv.hlen(args[0]))

            elif cmd == "HINCRBY":
                amt = int(args[2]) if len(args) > 2 else 1
                val = self.kv.hincrby(args[0], args[1], amt)
                return RESPSerializer.integer(val)

            # 5. List Commands
            elif cmd == "LPUSH":
                count = self.kv.lpush(args[0], *args[1:])
                return RESPSerializer.integer(count)

            elif cmd == "RPUSH":
                count = self.kv.rpush(args[0], *args[1:])
                return RESPSerializer.integer(count)

            elif cmd == "LPOP":
                val = self.kv.lpop(args[0])
                return RESPSerializer.bulk_string(val)

            elif cmd == "RPOP":
                val = self.kv.rpop(args[0])
                return RESPSerializer.bulk_string(val)

            elif cmd == "LRANGE":
                items = self.kv.lrange(args[0], int(args[1]), int(args[2]))
                return RESPSerializer.array(items)

            elif cmd == "LLEN":
                return RESPSerializer.integer(self.kv.llen(args[0]))

            elif cmd == "LINDEX":
                val = self.kv.lindex(args[0], int(args[1]))
                return RESPSerializer.bulk_string(val)

            elif cmd == "LSET":
                res = self.kv.lset(args[0], int(args[1]), args[2])
                return RESPSerializer.ok() if res else RESPSerializer.error("index out of range")

            elif cmd == "LTRIM":
                self.kv.ltrim(args[0], int(args[1]), int(args[2]))
                return RESPSerializer.ok()

            # 6. Set Commands
            elif cmd == "SADD":
                added = self.kv.sadd(args[0], *args[1:])
                return RESPSerializer.integer(added)

            elif cmd == "SREM":
                count = self.kv.srem(args[0], *args[1:])
                return RESPSerializer.integer(count)

            elif cmd == "SMEMBERS":
                return RESPSerializer.array(list(self.kv.smembers(args[0])))

            elif cmd == "SISMEMBER":
                res = self.kv.sismember(args[0], args[1])
                return RESPSerializer.integer(1 if res else 0)

            elif cmd == "SCARD":
                return RESPSerializer.integer(self.kv.scard(args[0]))

            elif cmd == "SUNION":
                return RESPSerializer.array(list(self.kv.sunion(*args)))

            elif cmd == "SINTER":
                return RESPSerializer.array(list(self.kv.sinter(*args)))

            elif cmd == "SDIFF":
                return RESPSerializer.array(list(self.kv.sdiff(*args)))

            # 7. Sorted Set Commands
            elif cmd == "ZADD":
                key = args[0]
                mapping = {}
                for i in range(1, len(args), 2):
                    score = float(args[i])
                    member = args[i + 1]
                    mapping[member] = score
                added = self.kv.zadd(key, mapping)
                return RESPSerializer.integer(added)

            elif cmd == "ZREM":
                count = self.kv.zrem(args[0], *args[1:])
                return RESPSerializer.integer(count)

            elif cmd == "ZSCORE":
                score = self.kv.zscore(args[0], args[1])
                return RESPSerializer.bulk_string(str(score) if score is not None else None)

            elif cmd == "ZINCRBY":
                score = self.kv.zincrby(args[0], float(args[1]), args[2])
                return RESPSerializer.bulk_string(str(score))

            elif cmd == "ZRANK":
                rank = self.kv.zrank(args[0], args[1])
                return RESPSerializer.integer(rank) if rank is not None else RESPSerializer.bulk_string(None)

            elif cmd == "ZRANGE":
                key = args[0]
                start, stop = int(args[1]), int(args[2])
                withscores = len(args) > 3 and args[3].upper() == "WITHSCORES"
                items = self.kv.zrange(key, start, stop, withscores=withscores)
                if withscores:
                    flat = []
                    for m, s in items:
                        flat.append(m)
                        flat.append(str(s))
                    return RESPSerializer.array(flat)
                return RESPSerializer.array(items)

            elif cmd == "ZCARD":
                return RESPSerializer.integer(self.kv.zcard(args[0]))

            # 8. Extended Zenith Commands: Full-Text BM25, Vector, Document
            elif cmd == "SEARCH.BM25":
                ns = args[0]
                query = args[1]
                limit = int(args[2]) if len(args) > 2 else 10
                idx = self._get_text_index(ns)
                results = idx.search(query, limit=limit)
                return RESPSerializer.bulk_string(json.dumps(results))

            elif cmd == "VECTOR.SEARCH":
                ns = args[0]
                vec_floats = [float(x) for x in args[1].split(",")]
                top_k = int(args[2]) if len(args) > 2 else 10
                metric = args[3] if len(args) > 3 else "cosine"
                vidx = self._get_vector_index(ns)
                results = vidx.search(vec_floats, top_k=top_k, metric=metric)
                return RESPSerializer.bulk_string(json.dumps(results))

            elif cmd == "DOC.INSERT":
                coll, doc_id, doc_json = args[0], args[1], args[2]
                doc = json.loads(doc_json)
                saved = self.doc_store.insert(coll, doc_id, doc)
                return RESPSerializer.bulk_string(json.dumps(saved))

            elif cmd == "DOC.GET":
                coll, doc_id = args[0], args[1]
                doc = self.doc_store.get(coll, doc_id)
                return RESPSerializer.bulk_string(json.dumps(doc) if doc else None)

            elif cmd == "DOC.QUERY":
                coll = args[0]
                filter_json = json.loads(args[1]) if len(args) > 1 else None
                docs = self.doc_store.query(coll, filter_dict=filter_json)
                return RESPSerializer.bulk_string(json.dumps(docs))

            elif cmd == "DOC.DELETE":
                coll, doc_id = args[0], args[1]
                res = self.doc_store.delete(coll, doc_id)
                return RESPSerializer.integer(1 if res else 0)

            else:
                return RESPSerializer.error(f"unknown command '{cmd}'")

        except Exception as e:
            return RESPSerializer.error(str(e))

    async def start(self) -> None:
        """Starts the TCP server."""
        self._server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )

    async def serve_forever(self) -> None:
        """Runs the server event loop."""
        if not self._server:
            await self.start()
        async with self._server:
            await self._server.serve_forever()

    def stop(self) -> None:
        if self._server:
            self._server.close()

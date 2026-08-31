"""
ZenithDB Multi-Type Key-Value Engine
Supports Strings, Hashes, Lists, Sets, and Sorted Sets (ZSets) with TTL expiration.
"""

import fnmatch
import heapq
import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from zenith.storage.lsm import LSMTree


class KeyValueEngine:
    """
    High-level key-value data structure engine.
    
    Data types are serialized into the underlying LSM-Tree using typed internal prefixes:
    - String: raw value or serialized json
    - Hash: __H__:{key}:{field}
    - List: __L__:{key}:{index} and metadata
    - Set: __S__:{key}:{member}
    - ZSet: __Z__:{key}:{member} and score
    - TTL: __EXP__:{key}
    """

    def __init__(self, lsm: LSMTree) -> None:
        self.lsm = lsm
        self._ttl_heap: List[Tuple[float, str]] = []  # Min-heap of (expire_timestamp, key)

    def _is_expired(self, key: str) -> bool:
        """Checks if a key has expired and lazily purges it without recursion."""
        exp_bytes = self.lsm.get(f"__EXP__:{key}".encode("utf-8"))
        if exp_bytes is not None:
            try:
                exp_ts = float(exp_bytes.decode("utf-8"))
                if time.time() >= exp_ts:
                    self._purge_key_internal(key)
                    return True
            except ValueError:
                pass
        return False

    def _purge_key_internal(self, key: str) -> None:
        """Directly removes all keys and metadata associated with key without checking expiration."""
        t_bytes = self.lsm.get(f"__TYPE__:{key}".encode("utf-8"))
        if not t_bytes:
            self.lsm.delete(f"__EXP__:{key}".encode("utf-8"))
            return

        t = t_bytes.decode("utf-8")
        self.lsm.delete(f"__TYPE__:{key}".encode("utf-8"))
        self.lsm.delete(f"__EXP__:{key}".encode("utf-8"))

        if t == "string":
            self.lsm.delete(f"__V__:{key}".encode("utf-8"))
        elif t == "hash":
            prefix = f"__H__:{key}:".encode("utf-8")
            for k, _ in self.lsm.scan(start_key=prefix):
                if not k.startswith(prefix):
                    break
                self.lsm.delete(k)
        elif t == "list":
            head, tail = self._get_list_meta(key)
            for i in range(head, tail):
                self.lsm.delete(f"__L__:{key}:{i}".encode("utf-8"))
            self.lsm.delete(f"__LMETA__:{key}".encode("utf-8"))
        elif t == "set":
            prefix = f"__S__:{key}:".encode("utf-8")
            for k, _ in self.lsm.scan(start_key=prefix):
                if not k.startswith(prefix):
                    break
                self.lsm.delete(k)
        elif t == "zset":
            prefix = f"__Z__:{key}:".encode("utf-8")
            for k, _ in self.lsm.scan(start_key=prefix):
                if not k.startswith(prefix):
                    break
                self.lsm.delete(k)

    # ------------------ STRING OPERATIONS ------------------ #

    def set(
        self,
        key: str,
        value: Union[str, bytes, int, float, dict, list],
        ex: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """
        Sets a string key to a value.
        - ex: TTL in seconds
        - nx: Only set if key does NOT exist
        - xx: Only set if key DOES exist
        """
        is_exp = self._is_expired(key)
        existing = None if is_exp else self.get(key)
        if nx and existing is not None:
            return False
        if xx and existing is None:
            return False

        if isinstance(value, bytes):
            payload = value
        elif isinstance(value, str):
            payload = value.encode("utf-8")
        else:
            payload = json.dumps(value).encode("utf-8")

        self.lsm.put(f"__V__:{key}".encode("utf-8"), payload)
        self.lsm.put(f"__TYPE__:{key}".encode("utf-8"), b"string")

        if ex is not None and ex > 0:
            self.expire(key, ex)
        else:
            self.persist(key)

        return True

    def setnx(self, key: str, value: Any) -> bool:
        """Sets key only if it does not already exist."""
        return self.set(key, value, nx=True)

    def get(self, key: str) -> Optional[Union[str, bytes]]:
        """Gets string value for a key."""
        if self._is_expired(key):
            return None

        val = self.lsm.get(f"__V__:{key}".encode("utf-8"))
        if val is None:
            return None
        try:
            return val.decode("utf-8")
        except UnicodeDecodeError:
            return val

    def mget(self, keys: List[str]) -> List[Optional[Union[str, bytes]]]:
        """Gets multiple keys in a single call."""
        return [self.get(k) for k in keys]

    def mset(self, mapping: Dict[str, Union[str, bytes, int, float]]) -> bool:
        """Sets multiple key-value pairs."""
        for k, v in mapping.items():
            self.set(k, v)
        return True

    def incrby(self, key: str, amount: int = 1) -> int:
        """Increments integer value of a key."""
        val = self.get(key)
        if val is None:
            new_val = amount
        else:
            try:
                new_val = int(val) + amount
            except ValueError:
                raise TypeError("Value is not an integer or out of range")
        self.set(key, str(new_val))
        return new_val

    def decrby(self, key: str, amount: int = 1) -> int:
        """Decrements integer value of a key."""
        return self.incrby(key, -amount)

    def append(self, key: str, val_to_append: str) -> int:
        """Appends a string to an existing string value."""
        curr = self.get(key) or ""
        new_val = str(curr) + str(val_to_append)
        self.set(key, new_val)
        return len(new_val)

    def strlen(self, key: str) -> int:
        """Returns string length of a key's value."""
        val = self.get(key)
        return len(val) if val is not None else 0

    # ------------------ HASH OPERATIONS ------------------ #

    def hset(self, key: str, field: str, value: Any) -> int:
        """Sets field in the hash stored at key."""
        self._is_expired(key)
        f_key = f"__H__:{key}:{field}".encode("utf-8")
        is_new = 1 if self.lsm.get(f_key) is None else 0

        val_bytes = str(value).encode("utf-8")
        self.lsm.put(f_key, val_bytes)
        self.lsm.put(f"__TYPE__:{key}".encode("utf-8"), b"hash")
        return is_new

    def hget(self, key: str, field: str) -> Optional[str]:
        """Gets value of field in hash stored at key."""
        if self._is_expired(key):
            return None
        f_key = f"__H__:{key}:{field}".encode("utf-8")
        val = self.lsm.get(f_key)
        return val.decode("utf-8") if val is not None else None

    def hdel(self, key: str, *fields: str) -> int:
        """Deletes one or more fields from hash."""
        count = 0
        for f in fields:
            f_key = f"__H__:{key}:{f}".encode("utf-8")
            if self.lsm.get(f_key) is not None:
                self.lsm.delete(f_key)
                count += 1
        return count

    def hgetall(self, key: str) -> Dict[str, str]:
        """Gets all fields and values in hash."""
        if self._is_expired(key):
            return {}
        prefix = f"__H__:{key}:".encode("utf-8")
        result = {}
        for k, v in self.lsm.scan(start_key=prefix):
            if not k.startswith(prefix):
                break
            field_name = k[len(prefix) :].decode("utf-8")
            result[field_name] = v.decode("utf-8")
        return result

    def hkeys(self, key: str) -> List[str]:
        """Returns all field names in hash."""
        return list(self.hgetall(key).keys())

    def hvals(self, key: str) -> List[str]:
        """Returns all values in hash."""
        return list(self.hgetall(key).values())

    def hexists(self, key: str, field: str) -> bool:
        """Checks if field exists in hash."""
        return self.hget(key, field) is not None

    def hlen(self, key: str) -> int:
        """Returns number of fields in hash."""
        return len(self.hgetall(key))

    def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        """Increments the integer value of a hash field."""
        curr = self.hget(key, field)
        if curr is None:
            new_val = amount
        else:
            try:
                new_val = int(curr) + amount
            except ValueError:
                raise TypeError("Hash field value is not an integer")
        self.hset(key, field, str(new_val))
        return new_val

    # ------------------ LIST OPERATIONS ------------------ #

    def _get_list_meta(self, key: str) -> Tuple[int, int]:
        """Returns (head_index, tail_index) for list."""
        meta_key = f"__LMETA__:{key}".encode("utf-8")
        val = self.lsm.get(meta_key)
        if val is None:
            return (0, 0)
        try:
            head, tail = map(int, val.decode("utf-8").split(":"))
            return (head, tail)
        except Exception:
            return (0, 0)

    def _set_list_meta(self, key: str, head: int, tail: int) -> None:
        meta_key = f"__LMETA__:{key}".encode("utf-8")
        self.lsm.put(meta_key, f"{head}:{tail}".encode("utf-8"))
        self.lsm.put(f"__TYPE__:{key}".encode("utf-8"), b"list")

    def lpush(self, key: str, *values: Any) -> int:
        """Prepends one or multiple values to a list."""
        self._is_expired(key)
        head, tail = self._get_list_meta(key)
        for v in values:
            head -= 1
            item_key = f"__L__:{key}:{head}".encode("utf-8")
            self.lsm.put(item_key, str(v).encode("utf-8"))
        self._set_list_meta(key, head, tail)
        return tail - head

    def rpush(self, key: str, *values: Any) -> int:
        """Appends one or multiple values to a list."""
        self._is_expired(key)
        head, tail = self._get_list_meta(key)
        for v in values:
            item_key = f"__L__:{key}:{tail}".encode("utf-8")
            self.lsm.put(item_key, str(v).encode("utf-8"))
            tail += 1
        self._set_list_meta(key, head, tail)
        return tail - head

    def lpop(self, key: str) -> Optional[str]:
        """Removes and returns the first element of a list."""
        if self._is_expired(key):
            return None
        head, tail = self._get_list_meta(key)
        if head >= tail:
            return None
        item_key = f"__L__:{key}:{head}".encode("utf-8")
        val = self.lsm.get(item_key)
        self.lsm.delete(item_key)
        head += 1
        self._set_list_meta(key, head, tail)
        return val.decode("utf-8") if val else None

    def rpop(self, key: str) -> Optional[str]:
        """Removes and returns the last element of a list."""
        if self._is_expired(key):
            return None
        head, tail = self._get_list_meta(key)
        if head >= tail:
            return None
        tail -= 1
        item_key = f"__L__:{key}:{tail}".encode("utf-8")
        val = self.lsm.get(item_key)
        self.lsm.delete(item_key)
        self._set_list_meta(key, head, tail)
        return val.decode("utf-8") if val else None

    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """Returns the specified elements of the list stored at key."""
        if self._is_expired(key):
            return []
        head, tail = self._get_list_meta(key)
        length = tail - head
        if length <= 0:
            return []

        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        else:
            stop = min(length - 1, stop)

        if start > stop or start >= length:
            return []

        items = []
        for i in range(head + start, head + stop + 1):
            item_key = f"__L__:{key}:{i}".encode("utf-8")
            val = self.lsm.get(item_key)
            if val is not None:
                items.append(val.decode("utf-8"))
        return items

    def llen(self, key: str) -> int:
        """Returns the length of the list."""
        if self._is_expired(key):
            return 0
        head, tail = self._get_list_meta(key)
        return max(0, tail - head)

    def lindex(self, key: str, index: int) -> Optional[str]:
        """Gets element at index."""
        items = self.lrange(key, index, index)
        return items[0] if items else None

    def lset(self, key: str, index: int, value: Any) -> bool:
        """Sets element at index in list."""
        if self._is_expired(key):
            return False
        head, tail = self._get_list_meta(key)
        length = tail - head
        if length <= 0:
            return False
        if index < 0:
            index = length + index
        if index < 0 or index >= length:
            return False
        item_key = f"__L__:{key}:{head + index}".encode("utf-8")
        self.lsm.put(item_key, str(value).encode("utf-8"))
        return True

    def ltrim(self, key: str, start: int, stop: int) -> bool:
        """Trims list to specified range."""
        if self._is_expired(key):
            return False
        head, tail = self._get_list_meta(key)
        length = tail - head
        if length <= 0:
            return True

        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        else:
            stop = min(length - 1, stop)

        if start > stop or start >= length:
            # Delete all
            self.delete(key)
            return True

        # Delete elements before start
        for i in range(head, head + start):
            self.lsm.delete(f"__L__:{key}:{i}".encode("utf-8"))
        # Delete elements after stop
        for i in range(head + stop + 1, tail):
            self.lsm.delete(f"__L__:{key}:{i}".encode("utf-8"))

        new_head = head + start
        new_tail = head + stop + 1
        self._set_list_meta(key, new_head, new_tail)
        return True

    # ------------------ SET OPERATIONS ------------------ #

    def sadd(self, key: str, *members: Any) -> int:
        """Adds one or more members to a set."""
        self._is_expired(key)
        added = 0
        for m in members:
            m_key = f"__S__:{key}:{m}".encode("utf-8")
            if self.lsm.get(m_key) is None:
                self.lsm.put(m_key, b"1")
                added += 1
        self.lsm.put(f"__TYPE__:{key}".encode("utf-8"), b"set")
        return added

    def srem(self, key: str, *members: Any) -> int:
        """Removes one or more members from a set."""
        count = 0
        for m in members:
            m_key = f"__S__:{key}:{m}".encode("utf-8")
            if self.lsm.get(m_key) is not None:
                self.lsm.delete(m_key)
                count += 1
        return count

    def smembers(self, key: str) -> Set[str]:
        """Returns all members of the set."""
        if self._is_expired(key):
            return set()
        prefix = f"__S__:{key}:".encode("utf-8")
        members = set()
        for k, _ in self.lsm.scan(start_key=prefix):
            if not k.startswith(prefix):
                break
            member_name = k[len(prefix) :].decode("utf-8")
            members.add(member_name)
        return members

    def sismember(self, key: str, member: Any) -> bool:
        """Checks if member is in set."""
        if self._is_expired(key):
            return False
        m_key = f"__S__:{key}:{member}".encode("utf-8")
        return self.lsm.get(m_key) is not None

    def scard(self, key: str) -> int:
        """Returns cardinality (number of elements) of set."""
        return len(self.smembers(key))

    def sunion(self, *keys: str) -> Set[str]:
        """Computes union of multiple sets."""
        result = set()
        for k in keys:
            result.update(self.smembers(k))
        return result

    def sinter(self, *keys: str) -> Set[str]:
        """Computes intersection of multiple sets."""
        if not keys:
            return set()
        result = self.smembers(keys[0])
        for k in keys[1:]:
            result.intersection_update(self.smembers(k))
        return result

    def sdiff(self, *keys: str) -> Set[str]:
        """Computes difference of multiple sets."""
        if not keys:
            return set()
        result = set(self.smembers(keys[0]))
        for k in keys[1:]:
            result.difference_update(self.smembers(k))
        return result

    # ------------------ SORTED SET (ZSET) OPERATIONS ------------------ #

    def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        """Adds all specified members with scores to sorted set."""
        self._is_expired(key)
        added = 0
        for member, score in mapping.items():
            m_key = f"__Z__:{key}:{member}".encode("utf-8")
            if self.lsm.get(m_key) is None:
                added += 1
            self.lsm.put(m_key, str(float(score)).encode("utf-8"))
        self.lsm.put(f"__TYPE__:{key}".encode("utf-8"), b"zset")
        return added

    def zrem(self, key: str, *members: str) -> int:
        """Removes members from sorted set."""
        count = 0
        for m in members:
            m_key = f"__Z__:{key}:{m}".encode("utf-8")
            if self.lsm.get(m_key) is not None:
                self.lsm.delete(m_key)
                count += 1
        return count

    def zscore(self, key: str, member: str) -> Optional[float]:
        """Returns the score of member in sorted set."""
        if self._is_expired(key):
            return None
        m_key = f"__Z__:{key}:{member}".encode("utf-8")
        val = self.lsm.get(m_key)
        return float(val.decode("utf-8")) if val is not None else None

    def zincrby(self, key: str, amount: float, member: str) -> float:
        """Increments the score of member in sorted set."""
        curr = self.zscore(key, member) or 0.0
        new_score = curr + amount
        self.zadd(key, {member: new_score})
        return new_score

    def zrank(self, key: str, member: str) -> Optional[int]:
        """Returns the 0-based rank of member ordered by ascending score."""
        members = self.zrange(key, 0, -1)
        try:
            return members.index(member)
        except ValueError:
            return None

    def zrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> List[Union[str, Tuple[str, float]]]:
        """Returns members sorted by ascending score."""
        if self._is_expired(key):
            return []
        prefix = f"__Z__:{key}:".encode("utf-8")
        items = []
        for k, v in self.lsm.scan(start_key=prefix):
            if not k.startswith(prefix):
                break
            member = k[len(prefix) :].decode("utf-8")
            score = float(v.decode("utf-8"))
            items.append((member, score))

        items.sort(key=lambda x: (x[1], x[0]))
        length = len(items)
        if length == 0:
            return []

        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        else:
            stop = min(length - 1, stop)

        sliced = items[start : stop + 1] if start <= stop else []
        if withscores:
            return sliced
        return [m for m, _ in sliced]

    def zcard(self, key: str) -> int:
        """Returns number of elements in sorted set."""
        return len(self.zrange(key, 0, -1))

    # ------------------ KEY MANAGEMENT & TTL ------------------ #

    def expire(self, key: str, seconds: int) -> bool:
        """Sets TTL on a key."""
        exp_ts = time.time() + max(0, seconds)
        self.lsm.put(f"__EXP__:{key}".encode("utf-8"), str(exp_ts).encode("utf-8"))
        heapq.heappush(self._ttl_heap, (exp_ts, key))
        return True

    def ttl(self, key: str) -> int:
        """Returns remaining TTL in seconds (-2: not found, -1: no TTL)."""
        if self._is_expired(key):
            return -2
        t_bytes = self.lsm.get(f"__TYPE__:{key}".encode("utf-8"))
        if t_bytes is None:
            return -2

        exp_bytes = self.lsm.get(f"__EXP__:{key}".encode("utf-8"))
        if exp_bytes is None:
            return -1
        try:
            exp_ts = float(exp_bytes.decode("utf-8"))
            remaining = int(exp_ts - time.time())
            return max(0, remaining) if remaining >= 0 else -2
        except ValueError:
            return -1

    def persist(self, key: str) -> bool:
        """Removes expiration from key."""
        exp_key = f"__EXP__:{key}".encode("utf-8")
        if self.lsm.get(exp_key) is not None:
            self.lsm.delete(exp_key)
            return True
        return False

    def exists(self, *keys: str) -> int:
        """Returns count of existing non-expired keys."""
        count = 0
        for k in keys:
            if not self._is_expired(k):
                t_bytes = self.lsm.get(f"__TYPE__:{k}".encode("utf-8"))
                if t_bytes is not None:
                    count += 1
        return count

    def type(self, key: str) -> str:
        """Returns the data type of the key (string, hash, list, set, zset, none)."""
        if self._is_expired(key):
            return "none"
        t_bytes = self.lsm.get(f"__TYPE__:{key}".encode("utf-8"))
        return t_bytes.decode("utf-8") if t_bytes else "none"

    def delete(self, *keys: str) -> int:
        """Deletes one or more keys and all sub-structures."""
        count = 0
        for key in keys:
            t_bytes = self.lsm.get(f"__TYPE__:{key}".encode("utf-8"))
            if t_bytes is not None:
                self._purge_key_internal(key)
                count += 1
        return count

    def keys(self, pattern: str = "*") -> List[str]:
        """Returns all matching keys."""
        matched = []
        prefix = b"__TYPE__:"
        for k, _ in self.lsm.scan(start_key=prefix):
            if not k.startswith(prefix):
                break
            key_name = k[len(prefix) :].decode("utf-8")
            if not self._is_expired(key_name):
                if fnmatch.fnmatch(key_name, pattern):
                    matched.append(key_name)
        return matched

    def dbsize(self) -> int:
        """Returns total count of live keys in database."""
        return len(self.keys("*"))

    def flushdb(self) -> None:
        """Clears all keys in the database."""
        for k in self.keys("*"):
            self.delete(k)
        self.lsm.flush()

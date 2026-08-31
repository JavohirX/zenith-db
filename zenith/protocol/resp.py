"""
ZenithDB RESP (Redis Serialization Protocol) Parser & Serializer
Full RESP2/RESP3 streaming parser and serializer.
"""

from typing import Any, List, Optional, Tuple, Union


class RESPParser:
    """
    Streaming parser for Redis Serialization Protocol (RESP).
    Can parse partial byte streams across multiple TCP packet fragments.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> None:
        """Appends raw TCP bytes to the parsing buffer."""
        self._buffer.extend(data)

    def get_next(self) -> Optional[Any]:
        """
        Attempts to parse the next complete command/object from the buffer.
        Returns None if buffer has incomplete data.
        """
        if not self._buffer:
            return None

        val, consumed = self._parse_from(0)
        if consumed > 0:
            del self._buffer[:consumed]
            return val
        return None

    def _parse_from(self, offset: int) -> Tuple[Optional[Any], int]:
        if offset >= len(self._buffer):
            return None, 0

        prefix = chr(self._buffer[offset])
        crlf_pos = self._buffer.find(b"\r\n", offset)
        if crlf_pos == -1:
            return None, 0

        line = self._buffer[offset + 1 : crlf_pos].decode("utf-8", errors="replace")
        header_len = (crlf_pos - offset) + 2

        # 1. Simple String (+)
        if prefix == "+":
            return line, header_len

        # 2. Error (-)
        elif prefix == "-":
            return Exception(line), header_len

        # 3. Integer (:)
        elif prefix == ":":
            try:
                return int(line), header_len
            except ValueError:
                return 0, header_len

        # 4. Bulk String ($)
        elif prefix == "$":
            try:
                str_len = int(line)
            except ValueError:
                return None, header_len

            if str_len == -1:
                return None, header_len  # Null bulk string

            total_needed = offset + header_len + str_len + 2
            if len(self._buffer) < total_needed:
                return None, 0  # Incomplete bulk string data

            data_start = offset + header_len
            data_end = data_start + str_len
            payload = bytes(self._buffer[data_start:data_end])
            try:
                result = payload.decode("utf-8")
            except UnicodeDecodeError:
                result = payload

            return result, (total_needed - offset)

        # 5. Array (*)
        elif prefix == "*":
            try:
                array_len = int(line)
            except ValueError:
                return None, header_len

            if array_len == -1:
                return None, header_len  # Null array

            elements = []
            curr_offset = offset + header_len

            for _ in range(array_len):
                elem, consumed = self._parse_from(curr_offset)
                if consumed == 0:
                    return None, 0  # Incomplete element in array
                elements.append(elem)
                curr_offset += consumed

            return elements, (curr_offset - offset)

        # 6. Inline Command fallback (e.g. "PING\r\n")
        else:
            full_line = self._buffer[offset:crlf_pos].decode(
                "utf-8", errors="replace"
            )
            tokens = full_line.strip().split()
            return tokens, (crlf_pos - offset) + 2


class RESPSerializer:
    """Serializes Python values to RESP2/RESP3 binary format."""

    @staticmethod
    def ok() -> bytes:
        return b"+OK\r\n"

    @staticmethod
    def ping() -> bytes:
        return b"+PONG\r\n"

    @staticmethod
    def simple_string(s: str) -> bytes:
        return f"+{s}\r\n".encode("utf-8")

    @staticmethod
    def error(msg: str) -> bytes:
        return f"-ERR {msg}\r\n".encode("utf-8")

    @staticmethod
    def integer(val: int) -> bytes:
        return f":{val}\r\n".encode("utf-8")

    @staticmethod
    def bulk_string(s: Optional[Union[str, bytes]]) -> bytes:
        if s is None:
            return b"$-1\r\n"
        if isinstance(s, str):
            b = s.encode("utf-8")
        else:
            b = s
        return f"${len(b)}\r\n".encode("utf-8") + b + b"\r\n"

    @staticmethod
    def array(items: Optional[List[Any]]) -> bytes:
        if items is None:
            return b"*-1\r\n"
        buf = bytearray(f"*{len(items)}\r\n".encode("utf-8"))
        for item in items:
            buf.extend(RESPSerializer.encode(item))
        return bytes(buf)

    @classmethod
    def encode(cls, val: Any) -> bytes:
        """Automatically serializes any Python object to RESP."""
        if val is None:
            return cls.bulk_string(None)
        elif isinstance(val, bool):
            return cls.integer(1 if val else 0)
        elif isinstance(val, int):
            return cls.integer(val)
        elif isinstance(val, (str, bytes)):
            return cls.bulk_string(val)
        elif isinstance(val, (list, tuple, set)):
            return cls.array(list(val))
        elif isinstance(val, dict):
            # Flatten dict to alternating array [k1, v1, k2, v2...]
            flat = []
            for k, v in val.items():
                flat.append(k)
                flat.append(v)
            return cls.array(flat)
        elif isinstance(val, Exception):
            return cls.error(str(val))
        else:
            return cls.bulk_string(str(val))

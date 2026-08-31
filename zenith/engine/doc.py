"""
ZenithDB Document Store Engine
JSON document storage, nested field extraction, secondary indexes, and rich query filtering.
"""

import json
from typing import Any, Callable, Dict, List, Optional, Set, Union
from zenith.storage.lsm import LSMTree


def get_nested_field(doc: Any, path: str) -> Any:
    """Extracts nested value using dot notation, e.g. 'user.address.city'."""
    tokens = path.split(".")
    curr = doc
    for token in tokens:
        if isinstance(curr, dict):
            curr = curr.get(token)
        elif isinstance(curr, list):
            try:
                idx = int(token)
                curr = curr[idx]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if curr is None:
            return None
    return curr


class DocumentStore:
    """
    JSON Document collection database with secondary indexing and filtering.
    """

    def __init__(self, lsm: LSMTree) -> None:
        self.lsm = lsm
        self._indexes: Dict[str, Set[str]] = {}  # collection -> set(field_paths)

    def _doc_key(self, collection: str, doc_id: str) -> bytes:
        return f"__DOC__:{collection}:{doc_id}".encode("utf-8")

    def _idx_key(
        self, collection: str, field_path: str, field_val: Any, doc_id: str
    ) -> bytes:
        val_str = json.dumps(field_val, sort_keys=True)
        return f"__DIDX__:{collection}:{field_path}:{val_str}:{doc_id}".encode("utf-8")

    def insert(
        self, collection: str, doc_id: str, document: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Inserts or overwrites a JSON document in the collection."""
        if not isinstance(document, dict):
            raise TypeError("Document must be a dict")

        doc = dict(document)
        doc["_id"] = doc_id

        # Read old doc to clean old secondary indexes
        old_doc = self.get(collection, doc_id)
        if old_doc:
            self._unindex_doc(collection, doc_id, old_doc)

        payload = json.dumps(doc).encode("utf-8")
        self.lsm.put(self._doc_key(collection, doc_id), payload)

        # Update indexes
        self._index_doc(collection, doc_id, doc)
        return doc

    def get(self, collection: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a document by collection and ID."""
        raw = self.lsm.get(self._doc_key(collection, doc_id))
        if raw is None:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def update(
        self, collection: str, doc_id: str, patch: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Updates fields of an existing document."""
        doc = self.get(collection, doc_id)
        if doc is None:
            return None

        doc.update(patch)
        return self.insert(collection, doc_id, doc)

    def delete(self, collection: str, doc_id: str) -> bool:
        """Deletes a document from the collection."""
        old_doc = self.get(collection, doc_id)
        if old_doc is None:
            return False

        self._unindex_doc(collection, doc_id, old_doc)
        self.lsm.delete(self._doc_key(collection, doc_id))
        return True

    def create_index(self, collection: str, field_path: str) -> None:
        """Creates a secondary index on a field path."""
        if collection not in self._indexes:
            self._indexes[collection] = set()
        self._indexes[collection].add(field_path)

        # Index existing documents
        for doc in self.query(collection, limit=1000000):
            val = get_nested_field(doc, field_path)
            if val is not None:
                self.lsm.put(
                    self._idx_key(collection, field_path, val, doc["_id"]), b"1"
                )

    def _index_doc(
        self, collection: str, doc_id: str, doc: Dict[str, Any]
    ) -> None:
        indexed_fields = self._indexes.get(collection, set())
        for field in indexed_fields:
            val = get_nested_field(doc, field)
            if val is not None:
                self.lsm.put(self._idx_key(collection, field, val, doc_id), b"1")

    def _unindex_doc(
        self, collection: str, doc_id: str, doc: Dict[str, Any]
    ) -> None:
        indexed_fields = self._indexes.get(collection, set())
        for field in indexed_fields:
            val = get_nested_field(doc, field)
            if val is not None:
                self.lsm.delete(self._idx_key(collection, field, val, doc_id))

    def _matches_filter(
        self, doc: Dict[str, Any], filter_dict: Dict[str, Any]
    ) -> bool:
        """Evaluates MongoDB-style query operators against document."""
        for path, condition in filter_dict.items():
            val = get_nested_field(doc, path)
            if isinstance(condition, dict):
                for op, target in condition.items():
                    if op == "$eq" and val != target:
                        return False
                    elif op == "$ne" and val == target:
                        return False
                    elif op == "$gt" and (val is None or val <= target):
                        return False
                    elif op == "$gte" and (val is None or val < target):
                        return False
                    elif op == "$lt" and (val is None or val >= target):
                        return False
                    elif op == "$lte" and (val is None or val > target):
                        return False
                    elif op == "$in" and val not in target:
                        return False
                    elif op == "$nin" and val in target:
                        return False
                    elif op == "$contains":
                        if not (
                            isinstance(val, (list, str, set)) and target in val
                        ):
                            return False
                    elif op == "$regex":
                        import re
                        if not (isinstance(val, str) and re.search(target, val)):
                            return False
            else:
                if val != condition:
                    return False
        return True

    def query(
        self,
        collection: str,
        filter_dict: Optional[Dict[str, Any]] = None,
        filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
        sort_by: Optional[str] = None,
        reverse: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Queries documents with optional filtering, sorting, and pagination."""
        prefix = f"__DOC__:{collection}:".encode("utf-8")
        results: List[Dict[str, Any]] = []

        for k, v in self.lsm.scan(start_key=prefix):
            if not k.startswith(prefix):
                break
            try:
                doc = json.loads(v.decode("utf-8"))
            except Exception:
                continue

            if filter_dict and not self._matches_filter(doc, filter_dict):
                continue
            if filter_fn and not filter_fn(doc):
                continue

            results.append(doc)

        # Sort if requested
        if sort_by:
            results.sort(
                key=lambda d: (
                    get_nested_field(d, sort_by) is None,
                    get_nested_field(d, sort_by),
                ),
                reverse=reverse,
            )

        # Paginate
        return results[offset : offset + limit]

    def count(self, collection: str) -> int:
        """Returns total document count in collection."""
        prefix = f"__DOC__:{collection}:".encode("utf-8")
        c = 0
        for k, _ in self.lsm.scan(start_key=prefix):
            if not k.startswith(prefix):
                break
            c += 1
        return c

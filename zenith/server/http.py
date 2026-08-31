"""
ZenithDB HTTP REST API and Live Web Dashboard
Embedded HTTP server with REST endpoints and real-time zero-dependency HTML5/CSS/SVG dashboard.
"""

import json
import os
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from zenith.engine.doc import DocumentStore
from zenith.engine.kv import KeyValueEngine
from zenith.engine.text import FullTextIndex
from zenith.engine.vector import VectorIndex
from zenith.storage.lsm import LSMTree

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ZenithDB Control Plane</title>
<style>
  :root {
    --bg-dark: #0f172a;
    --card-bg: #1e293b;
    --accent: #38bdf8;
    --accent-glow: rgba(56, 189, 248, 0.2);
    --text: #f8fafc;
    --text-dim: #94a3b8;
    --border: #334155;
    --success: #34d399;
    --warn: #fbbf24;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; }
  body { background: var(--bg-dark); color: var(--text); padding: 24px; }
  header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 24px; }
  .logo { font-size: 24px; font-weight: bold; color: var(--accent); display: flex; align-items: center; gap: 8px; }
  .badge { background: #0369a1; padding: 4px 10px; border-radius: 999px; font-size: 12px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 10px; padding: 20px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.3); }
  .card h3 { font-size: 13px; color: var(--text-dim); text-transform: uppercase; margin-bottom: 8px; letter-spacing: 0.5px; }
  .card .metric { font-size: 28px; font-weight: bold; color: var(--text); }
  .card .metric-sub { font-size: 12px; color: var(--success); margin-top: 4px; }
  .main-section { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  @media(max-width: 900px) { .main-section { grid-template-columns: 1fr; } }
  .panel { background: var(--card-bg); border: 1px solid var(--border); border-radius: 10px; padding: 20px; }
  .panel h2 { font-size: 16px; margin-bottom: 14px; color: var(--accent); }
  input, textarea, button, select { background: #0f172a; border: 1px solid var(--border); color: var(--text); padding: 10px 14px; border-radius: 6px; font-size: 14px; width: 100%; margin-bottom: 12px; }
  button { background: #0284c7; font-weight: bold; cursor: pointer; border: none; transition: background 0.2s; }
  button:hover { background: #0369a1; }
  pre { background: #090d16; border: 1px solid #1e293b; padding: 14px; border-radius: 6px; overflow-x: auto; font-size: 13px; max-height: 280px; color: #a5f3fc; }
  .key-list { max-height: 260px; overflow-y: auto; list-style: none; }
  .key-item { padding: 8px 12px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; font-size: 13px; }
</style>
</head>
<body>
  <header>
    <div class="logo">⚡ ZenithDB <span class="badge">Zero-Dependency v1.0.0</span></div>
    <div style="font-size: 13px; color: var(--text-dim);">LSM-Tree · BM25 Search · Vector Index · RESP</div>
  </header>

  <div class="grid">
    <div class="card">
      <h3>Active Keys</h3>
      <div class="metric" id="m-keys">0</div>
      <div class="metric-sub">LSM MemTable + SSTables</div>
    </div>
    <div class="card">
      <h3>Total Operations</h3>
      <div class="metric" id="m-ops">0</div>
      <div class="metric-sub">Real-time throughput</div>
    </div>
    <div class="card">
      <h3>SSTable Count</h3>
      <div class="metric" id="m-sst">0</div>
      <div class="metric-sub">On-disk tables</div>
    </div>
    <div class="card">
      <h3>Server Uptime</h3>
      <div class="metric" id="m-uptime">0s</div>
      <div class="metric-sub">Crash durable (WAL)</div>
    </div>
  </div>

  <div class="main-section">
    <div class="panel">
      <h2>Interactive Query Console</h2>
      <select id="op-type" onchange="updateForm()">
        <option value="get">GET Key</option>
        <option value="set">SET Key</option>
        <option value="del">DELETE Key</option>
        <option value="search">BM25 Full-Text Search</option>
        <option value="vector">Vector Cosine Search</option>
      </select>
      <input type="text" id="input-key" placeholder="Key or Document ID">
      <textarea id="input-val" placeholder="Value / JSON Payload" rows="3" style="display:none;"></textarea>
      <button onclick="executeOp()">Execute Operation</button>
      <pre id="output-result">Ready for queries.</pre>
    </div>

    <div class="panel">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
        <h2 style="margin:0;">Stored Keys Browser</h2>
        <button style="width:auto; padding:4px 12px; margin:0;" onclick="loadKeys()">Refresh</button>
      </div>
      <ul class="key-list" id="keys-container">
        <li class="key-item">Loading database keys...</li>
      </ul>
    </div>
  </div>

  <script>
    async function fetchMetrics() {
      try {
        const res = await fetch('/metrics');
        const data = await res.json();
        document.getElementById('m-keys').innerText = data.keys;
        document.getElementById('m-ops').innerText = data.total_ops;
        document.getElementById('m-sst').innerText = data.sstables;
        document.getElementById('m-uptime').innerText = data.uptime + 's';
      } catch (e) {}
    }

    async function loadKeys() {
      try {
        const res = await fetch('/api/v1/keys');
        const data = await res.json();
        const container = document.getElementById('keys-container');
        container.innerHTML = '';
        if (data.keys.length === 0) {
          container.innerHTML = '<li class="key-item" style="color:var(--text-dim);">No keys stored yet.</li>';
          return;
        }
        data.keys.forEach(k => {
          const li = document.createElement('li');
          li.className = 'key-item';
          li.innerHTML = `<span>${k}</span><a href="javascript:void(0)" onclick="quickGet('${k}')" style="color:var(--accent); text-decoration:none;">Inspect</a>`;
          container.appendChild(li);
        });
      } catch (e) {}
    }

    function updateForm() {
      const op = document.getElementById('op-type').value;
      const valBox = document.getElementById('input-val');
      const keyBox = document.getElementById('input-key');
      if (op === 'set' || op === 'search' || op === 'vector') {
        valBox.style.display = 'block';
      } else {
        valBox.style.display = 'none';
      }
      if (op === 'search') {
        keyBox.placeholder = 'Namespace (e.g. default)';
        valBox.placeholder = 'Search Query (e.g. database transactions)';
      } else if (op === 'vector') {
        keyBox.placeholder = 'Namespace (e.g. default)';
        valBox.placeholder = 'Floats comma-separated (e.g. 0.1, 0.8, -0.4)';
      } else {
        keyBox.placeholder = 'Key';
        valBox.placeholder = 'Value (String or JSON)';
      }
    }

    async function quickGet(k) {
      document.getElementById('op-type').value = 'get';
      document.getElementById('input-key').value = k;
      updateForm();
      await executeOp();
    }

    async function executeOp() {
      const op = document.getElementById('op-type').value;
      const key = document.getElementById('input-key').value;
      const val = document.getElementById('input-val').value;
      const out = document.getElementById('output-result');

      try {
        let res, json;
        if (op === 'get') {
          res = await fetch(`/api/v1/kv/${encodeURIComponent(key)}`);
          json = await res.json();
        } else if (op === 'set') {
          res = await fetch('/api/v1/kv', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({key: key, value: val})
          });
          json = await res.json();
          loadKeys();
        } else if (op === 'del') {
          res = await fetch(`/api/v1/kv/${encodeURIComponent(key)}`, {method: 'DELETE'});
          json = await res.json();
          loadKeys();
        } else if (op === 'search') {
          res = await fetch('/api/v1/search', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({namespace: key || 'default', query: val})
          });
          json = await res.json();
        } else if (op === 'vector') {
          const floats = val.split(',').map(Number);
          res = await fetch('/api/v1/vector/search', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({namespace: key || 'default', vector: floats})
          });
          json = await res.json();
        }
        out.innerText = JSON.stringify(json, null, 2);
        fetchMetrics();
      } catch (err) {
        out.innerText = 'Error: ' + err.message;
      }
    }

    setInterval(fetchMetrics, 2000);
    fetchMetrics();
    loadKeys();
  </script>
</body>
</html>
"""


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Multi-threaded standard library HTTP server."""
    daemon_threads = True


class ZenithHTTPHandler(BaseHTTPRequestHandler):
    """Handles REST API and dashboard HTTP requests."""

    server_instance: "HTTPServerWrapper"

    def _send_json(
        self, data: Any, status: int = HTTPStatus.OK
    ) -> None:
        payload = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(payload)

    def _send_html(self, html: str) -> None:
        payload = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        self.server_instance.total_ops += 1
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        query = parse_qs(parsed.query)

        # 1. Web Dashboard
        if path == "" or path == "/dashboard":
            self._send_html(DASHBOARD_HTML)
            return

        # 2. Health check
        if path == "/health":
            self._send_json({"status": "ok", "version": "1.0.0", "engine": "ZenithDB"})
            return

        # 3. Metrics
        if path == "/metrics":
            uptime = int(time.time() - self.server_instance.start_time)
            self._send_json(
                {
                    "keys": self.server_instance.kv.dbsize(),
                    "total_ops": self.server_instance.total_ops,
                    "sstables": len(self.server_instance.lsm._sst_readers),
                    "memtable_size_bytes": self.server_instance.lsm.memtable.byte_size,
                    "uptime": uptime,
                }
            )
            return

        # 4. List keys: /api/v1/keys
        if path == "/api/v1/keys":
            pattern = query.get("pattern", ["*"])[0]
            keys = self.server_instance.kv.keys(pattern)
            self._send_json({"keys": keys, "count": len(keys)})
            return

        # 5. Get key: /api/v1/kv/<key>
        if path.startswith("/api/v1/kv/"):
            key = path[len("/api/v1/kv/") :]
            val = self.server_instance.kv.get(key)
            if val is None:
                self._send_json({"error": "Key not found"}, status=HTTPStatus.NOT_FOUND)
            else:
                self._send_json(
                    {
                        "key": key,
                        "value": val,
                        "type": self.server_instance.kv.type(key),
                        "ttl": self.server_instance.kv.ttl(key),
                    }
                )
            return

        # 6. Get Document: /api/v1/doc/<coll>/<id>
        if path.startswith("/api/v1/doc/"):
            parts = path[len("/api/v1/doc/") :].split("/")
            if len(parts) == 2:
                coll, doc_id = parts[0], parts[1]
                doc = self.server_instance.doc_store.get(coll, doc_id)
                if doc is None:
                    self._send_json(
                        {"error": "Document not found"}, status=HTTPStatus.NOT_FOUND
                    )
                else:
                    self._send_json(doc)
                return

        self._send_json({"error": "Endpoint not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        self.server_instance.total_ops += 1
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        content_length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_length) if content_length > 0 else b""
        try:
            body = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
        except json.JSONDecodeError:
            self._send_json({"error": "Invalid JSON body"}, status=HTTPStatus.BAD_REQUEST)
            return

        # 1. Set key: /api/v1/kv
        if path == "/api/v1/kv":
            key = body.get("key")
            val = body.get("value")
            ex = body.get("ex")
            if not key or val is None:
                self._send_json(
                    {"error": "Missing 'key' or 'value'"}, status=HTTPStatus.BAD_REQUEST
                )
                return
            self.server_instance.kv.set(key, val, ex=ex)
            self._send_json({"status": "ok", "key": key})
            return

        # 2. BM25 Search: /api/v1/search
        if path == "/api/v1/search":
            ns = body.get("namespace", "default")
            query = body.get("query", "")
            limit = int(body.get("limit", 10))
            idx = self.server_instance._get_text_index(ns)
            results = idx.search(query, limit=limit)
            self._send_json({"results": results, "count": len(results)})
            return

        # 3. Vector Search: /api/v1/vector/search
        if path == "/api/v1/vector/search":
            ns = body.get("namespace", "default")
            vec = body.get("vector", [])
            top_k = int(body.get("top_k", 10))
            metric = body.get("metric", "cosine")
            vidx = self.server_instance._get_vector_index(ns)
            results = vidx.search(vec, top_k=top_k, metric=metric)
            self._send_json({"results": results, "count": len(results)})
            return

        # 4. Vector Insert: /api/v1/vector/insert
        if path == "/api/v1/vector/insert":
            ns = body.get("namespace", "default")
            vec_id = body.get("id")
            vec = body.get("vector", [])
            metadata = body.get("metadata", {})
            if not vec_id or not vec:
                self._send_json({"error": "Missing 'id' or 'vector'"}, status=HTTPStatus.BAD_REQUEST)
                return
            vidx = self.server_instance._get_vector_index(ns)
            vidx.insert(vec_id, vec, metadata)
            self._send_json({"status": "ok", "id": vec_id})
            return

        # 5. Document Query: /api/v1/doc/<coll>/query
        if path.startswith("/api/v1/doc/") and path.endswith("/query"):
            coll = path[len("/api/v1/doc/") : -len("/query")]
            filter_dict = body.get("filter")
            sort_by = body.get("sort_by")
            limit = int(body.get("limit", 100))
            offset = int(body.get("offset", 0))
            docs = self.server_instance.doc_store.query(
                coll, filter_dict=filter_dict, sort_by=sort_by, limit=limit, offset=offset
            )
            self._send_json({"results": docs, "count": len(docs)})
            return

        # 6. Document Insert: /api/v1/doc/<coll>
        if path.startswith("/api/v1/doc/"):
            coll = path[len("/api/v1/doc/") :]
            doc_id = body.get("_id") or f"doc_{int(time.time() * 1000)}"
            saved = self.server_instance.doc_store.insert(coll, doc_id, body)
            self._send_json(saved, status=HTTPStatus.CREATED)
            return

        # 7. Compaction: /api/v1/compact
        if path == "/api/v1/compact":
            self.server_instance.lsm.compact()
            self._send_json({"status": "Compaction completed"})
            return

        self._send_json({"error": "Endpoint not found"}, status=HTTPStatus.NOT_FOUND)

    def do_DELETE(self) -> None:
        self.server_instance.total_ops += 1
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        # Delete Key
        if path.startswith("/api/v1/kv/"):
            key = path[len("/api/v1/kv/") :]
            deleted = self.server_instance.kv.delete(key)
            self._send_json({"deleted": deleted > 0, "key": key})
            return

        # Delete Document
        if path.startswith("/api/v1/doc/"):
            parts = path[len("/api/v1/doc/") :].split("/")
            if len(parts) == 2:
                coll, doc_id = parts[0], parts[1]
                deleted = self.server_instance.doc_store.delete(coll, doc_id)
                self._send_json({"deleted": deleted, "collection": coll, "id": doc_id})
                return

        self._send_json({"error": "Endpoint not found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default stdout logging for clean CLI output."""
        pass


class HTTPServerWrapper:
    """Wrapper managing the multi-threaded HTTP server."""

    def __init__(
        self,
        lsm: LSMTree,
        host: str = "127.0.0.1",
        port: int = 8080,
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

        ZenithHTTPHandler.server_instance = self
        self._server = ThreadedHTTPServer((host, port), ZenithHTTPHandler)

    def _get_text_index(self, namespace: str) -> FullTextIndex:
        if namespace not in self.text_indexes:
            self.text_indexes[namespace] = FullTextIndex(self.lsm, namespace)
        return self.text_indexes[namespace]

    def _get_vector_index(self, namespace: str) -> VectorIndex:
        if namespace not in self.vector_indexes:
            self.vector_indexes[namespace] = VectorIndex(self.lsm, namespace)
        return self.vector_indexes[namespace]

    def serve_forever(self) -> None:
        self._server.serve_forever()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

import hashlib
import io
import json
import os
import time
import threading
import tempfile
import uuid
import shutil
from pathlib import Path
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from threading import Lock, Thread
from chunking import content_defined_chunking
import elasticsearch_index as es_index
import httpx
import psycopg
from collections import defaultdict, deque
from fastapi import (
    Depends, FastAPI, File, Header, HTTPException,
    Query, Request, UploadFile
)
from fastapi.responses import Response, StreamingResponse
from psycopg.types.json import Jsonb

app = FastAPI(title="Distributed Object Storage Gateway")
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"]
)
RATE_LIMIT = 120
RATE_WINDOW = 60

request_times = defaultdict(deque)
rate_lock = threading.Lock()


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.url.path.startswith("/internal/"):
        return await call_next(request)

    api_key = request.headers.get("x-api-key", "anonymous")
    now = time.time()

    with rate_lock:
        timestamps = request_times[api_key]

        while timestamps and now - timestamps[0] > RATE_WINDOW:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            return Response(
                content='{"detail":"Rate limit exceeded"}',
                status_code=429,
                media_type="application/json"
            )

        timestamps.append(now)

    return await call_next(request)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://storage:storagepass@postgres:5432/storage"
)
API_KEY = os.getenv("API_KEY", "midterm-secret")
CHUNK_SIZE = 1024 * 1024
REPLICATION = 3
WRITE_QUORUM = 2
READ_QUORUM = 2
HEARTBEAT_TIMEOUT = 15
UPLOAD_ROOT = Path(os.getenv("UPLOAD_ROOT", "/tmp/storage-multipart"))
UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)

NODES = {
    f"storage{i}": f"http://storage{i}:8000"
    for i in range(1, 5)
}

heartbeats = {}
heartbeat_lock = Lock()


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def db():
    return psycopg.connect(DATABASE_URL)


def sync_object_to_elasticsearch(bucket: str, key: str):
    """Best-effort post-commit index update; /search/elasticsearch/sync can reconcile."""
    try:
        with db() as conn:
            row = conn.execute(
                "SELECT size, version, created_at, tags FROM objects WHERE bucket=%s AND key=%s",
                (bucket, key)
            ).fetchone()
        if row:
            es_index.ensure_index()
            es_index.index_object(bucket, key, *row)
    except (httpx.HTTPError, ValueError) as exc:
        print(f"[ELASTICSEARCH] Index update pending for {bucket}/{key}: {exc}", flush=True)


def remove_object_from_elasticsearch(bucket: str, key: str):
    try:
        es_index.remove_object(bucket, key)
    except (httpx.HTTPError, ValueError) as exc:
        print(f"[ELASTICSEARCH] Delete update pending for {bucket}/{key}: {exc}", flush=True)


def require_key(x_api_key: str | None = Header(default=None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "Invalid or missing API key")


def initialize_database():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS objects (
                bucket TEXT NOT NULL REFERENCES buckets(name),
                key TEXT NOT NULL,
                size BIGINT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                chunk_hashes JSONB NOT NULL,
                tags JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY(bucket, key)
            )
        """)
        conn.execute("ALTER TABLE objects ADD COLUMN IF NOT EXISTS tags JSONB NOT NULL DEFAULT '{}'::jsonb")
        conn.execute("ALTER TABLE objects ADD COLUMN IF NOT EXISTS chunking TEXT NOT NULL DEFAULT 'fixed'")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                hash TEXT PRIMARY KEY,
                size BIGINT NOT NULL,
                ref_count BIGINT NOT NULL DEFAULT 0
            )
        """)


@app.on_event("startup")
def startup():
    for attempt in range(20):
        try:
            initialize_database()
            repair_thread = Thread(target=automatic_repair_loop, daemon=True)
            repair_thread.start()
            print("[AUTO REPAIR] Background worker started", flush=True)

            scrubber_thread = Thread(target=background_scrubber_loop, daemon=True)
            scrubber_thread.start()
            print("[SCRUBBER] Background integrity worker started", flush=True)
            return
        except psycopg.OperationalError:
            time.sleep(2)
    raise RuntimeError("PostgreSQL unavailable")


@app.post("/internal/heartbeat")
def receive_heartbeat(payload: dict):
    node_id = payload.get("node_id")

    if node_id not in NODES:
        raise HTTPException(400, "Unknown storage node")

    with heartbeat_lock:
        heartbeats[node_id] = {
            "last_seen": time.time(),
            "free_space": payload.get("free_space", 0),
            "chunk_count": payload.get("chunk_count", 0)
        }

    return {"status": "registered"}


def active_nodes():
    now = time.time()

    with heartbeat_lock:
        return sorted(
            node for node, info in heartbeats.items()
            if now - info["last_seen"] <= HEARTBEAT_TIMEOUT
        )


def ring_for(nodes):
    ring = []

    for node in nodes:
        for virtual_node in range(100):
            token = hashlib.sha256(
                f"{node}:{virtual_node}".encode()
            ).hexdigest()
            ring.append((int(token, 16), node))

    return sorted(ring)


def placement(chunk_hash: str, nodes=None, count=REPLICATION):
    nodes = active_nodes() if nodes is None else sorted(nodes)

    if not nodes:
        return []

    ring = ring_for(nodes)
    position = int(chunk_hash, 16)
    start = 0

    while start < len(ring) and ring[start][0] < position:
        start += 1

    selected = []

    for offset in range(len(ring)):
        node = ring[(start + offset) % len(ring)][1]

        if node not in selected:
            selected.append(node)

        if len(selected) == min(count, len(nodes)):
            break

    return selected


@app.get("/cluster/status", dependencies=[Depends(require_key)])
def cluster_status(scan_objects: bool = False):
    healthy = active_nodes()

    with heartbeat_lock:
        nodes = [
            {
                "id": node,
                "healthy": node in healthy,
                "free_space": heartbeats.get(node, {}).get("free_space"),
                "chunk_count": heartbeats.get(node, {}).get("chunk_count")
            }
            for node in NODES
        ]

    with db() as conn:
        records = conn.execute(
            "SELECT bucket, key, chunk_hashes FROM objects ORDER BY bucket, key LIMIT 1000"
        ).fetchall() if scan_objects else []

    degraded = 0
    unavailable = 0
    # This checks actual valid copies, not just node liveness.
    with httpx.Client(timeout=3) as client:
        for bucket, key, hashes in records:
            object_degraded = False
            object_unavailable = False
            for chunk_hash in set(hashes):
                copies = 0
                for node in healthy:
                    try:
                        result = client.head(f"{NODES[node]}/chunks/{chunk_hash}")
                        if result.status_code == 200:
                            copies += 1
                    except httpx.HTTPError:
                        pass
                if copies < REPLICATION:
                    object_degraded = True
                if copies < READ_QUORUM:
                    object_unavailable = True
            degraded += int(object_degraded)
            unavailable += int(object_unavailable)

    return {
        "healthy_nodes": len(healthy),
        "total_nodes": len(NODES),
        "nodes": nodes,
        "degraded_objects": degraded if scan_objects else None,
        "unavailable_objects": unavailable if scan_objects else None,
        "object_scan_performed": scan_objects,
        "scan_limit": 1000 if scan_objects else None
    }


@app.get("/placement/{chunk_hash}", dependencies=[Depends(require_key)])
def chunk_placement(chunk_hash: str):
    if len(chunk_hash) != 64:
        raise HTTPException(400, "Invalid chunk hash")

    return {
        "hash": chunk_hash,
        "target_nodes": placement(chunk_hash)
    }


def write_chunk(chunk_hash: str, data: bytes):
    targets = placement(chunk_hash)

    if len(targets) < REPLICATION:
        raise HTTPException(
            503, "Not enough healthy nodes for replication"
        )

    successful = []

    with httpx.Client(timeout=30) as client:
        for node in targets:
            try:
                response = client.put(
                    f"{NODES[node]}/chunks/{chunk_hash}",
                    content=data
                )
                if response.status_code == 200:
                    successful.append(node)
            except httpx.HTTPError:
                pass

    if len(successful) < WRITE_QUORUM:
        raise HTTPException(503, "Write quorum not reached")

    return successful


def read_chunk(chunk_hash: str):
    # Prefer current placement, but search surviving nodes
    # when the ring changes after a failure.
    preferred = placement(chunk_hash)
    candidates = preferred + [
        node for node in active_nodes() if node not in preferred
    ]

    valid_data = None
    valid_count = 0

    with httpx.Client(timeout=15) as client:
        for node in candidates:
            try:
                response = client.get(
                    f"{NODES[node]}/chunks/{chunk_hash}"
                )

                if (
                    response.status_code == 200
                    and sha256(response.content) == chunk_hash
                ):
                    valid_data = response.content
                    valid_count += 1

                    if valid_count >= READ_QUORUM:
                        return valid_data
            except httpx.HTTPError:
                continue

    raise HTTPException(
        503,
        f"Read quorum not reached for chunk {chunk_hash}"
    )


def cleanup_unreferenced_chunks():
    """Clean zero-reference chunks (run only while writes/deletes are paused).

    A chunk's database record is retained when any node cannot confirm
    deletion, allowing the next cleanup run to retry it.
    """
    cleaned = 0
    pending = 0

    # Lock the candidate rows while deleting their replicas so a concurrent
    # metadata transaction cannot increment these reference counts.
    # For this prototype, pause all uploads and deletions during cleanup.
    with db() as conn:
        rows = conn.execute(
            "SELECT hash FROM chunks WHERE ref_count <= 0 FOR UPDATE"
        ).fetchall()

        with httpx.Client(timeout=10) as client:
            for (chunk_hash,) in rows:
                confirmed = True
                for node_url in NODES.values():
                    try:
                        response = client.delete(
                            f"{node_url}/chunks/{chunk_hash}"
                        )
                        # 404 means the chunk was already absent on this node.
                        if response.status_code not in (200, 204, 404):
                            confirmed = False
                    except httpx.HTTPError:
                        confirmed = False

                if confirmed:
                    result = conn.execute(
                        "DELETE FROM chunks WHERE hash=%s AND ref_count <= 0",
                        (chunk_hash,),
                    )
                    cleaned += result.rowcount
                else:
                    pending += 1

    return {"cleaned_chunks": cleaned, "pending_chunks": pending}


@app.post("/cluster/cleanup", dependencies=[Depends(require_key)])
def cleanup_cluster():
    return cleanup_unreferenced_chunks()


@app.post("/buckets/{bucket}", dependencies=[Depends(require_key)])
def create_bucket(bucket: str):
    with db() as conn:
        conn.execute(
            "INSERT INTO buckets(name) VALUES (%s) ON CONFLICT DO NOTHING",
            (bucket,)
        )

    return {"bucket": bucket, "status": "created"}


@app.get("/buckets", dependencies=[Depends(require_key)])
def list_buckets():
    with db() as conn:
        rows = conn.execute(
            "SELECT name FROM buckets ORDER BY name"
        ).fetchall()

    return {"buckets": [row[0] for row in rows]}


@app.put(
    "/objects/{bucket}/{key:path}",
    dependencies=[Depends(require_key)]
)
async def upload_object(
    bucket: str,
    key: str,
    file: UploadFile = File(...),
    tags_json: str = "{}",
    chunking: str = "fixed"
):
    return await store_object(
        bucket, key, file, tags_json, chunking
    )

async def store_object(
    bucket: str,
    key: str,
    file: UploadFile,
    tags_json: str = "{}",
    chunking: str = "fixed"
):
    try:
        tags = json.loads(tags_json)
        if not isinstance(tags, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in tags.items()):
            raise ValueError("Tags must be string-to-string JSON object")
    except (ValueError, TypeError):
        raise HTTPException(400, "tags_json must be a JSON object of string tags")
    if not key:
        raise HTTPException(400, "Object key cannot be empty")

    chunks = []
    sizes = {}
    total_size = 0

    if chunking not in ("fixed", "cdc"):
        raise HTTPException(400, "chunking must be 'fixed' or 'cdc'")

    if chunking == "fixed":
        # Keep the existing memory-efficient fixed-size upload.
        while True:
            data = await file.read(CHUNK_SIZE)
            if not data:
                break

            chunk_hash = sha256(data)
            write_chunk(chunk_hash, data)
            chunks.append(chunk_hash)
            sizes[chunk_hash] = len(data)
            total_size += len(data)
    else:
        # Initial CDC integration: suitable for functional testing.
        # TODO: make CDC incremental before 100 MB–1 GB benchmarks.
        file_data = await file.read()
        for data in content_defined_chunking(file_data):
            chunk_hash = sha256(data)
            write_chunk(chunk_hash, data)
            chunks.append(chunk_hash)
            sizes[chunk_hash] = len(data)
            total_size += len(data)

    with db() as conn:
        bucket_exists = conn.execute(
            "SELECT 1 FROM buckets WHERE name=%s",
            (bucket,)
        ).fetchone()

        if not bucket_exists:
            raise HTTPException(404, "Bucket not found")

        old = conn.execute(
            "SELECT chunk_hashes, version FROM objects WHERE bucket=%s AND key=%s FOR UPDATE",
            (bucket, key)
        ).fetchone()

        if old:
            for old_hash in old[0]:
                conn.execute(
                    "UPDATE chunks SET ref_count=ref_count-1 WHERE hash=%s",
                    (old_hash,)
                )

        for chunk_hash in chunks:
            conn.execute("""
                INSERT INTO chunks(hash, size, ref_count)
                VALUES (%s, %s, 1)
                ON CONFLICT(hash)
                DO UPDATE SET ref_count=chunks.ref_count+1
            """, (chunk_hash, sizes[chunk_hash]))

        version = old[1] + 1 if old else 1

        conn.execute("""
            INSERT INTO objects(bucket, key, size, version, chunk_hashes, tags, chunking)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(bucket, key)
            DO UPDATE SET
                size=EXCLUDED.size,
                version=EXCLUDED.version,
                chunk_hashes=EXCLUDED.chunk_hashes,
                tags=EXCLUDED.tags,
                chunking=EXCLUDED.chunking,
                created_at=NOW()
        """, (bucket, key, total_size, version, Jsonb(chunks), Jsonb(tags), chunking))

    sync_object_to_elasticsearch(bucket, key)

    return {
        "bucket": bucket,
        "key": key,
        "size": total_size,
        "version": version,
        "chunks": len(chunks),
        "unique_chunks": len(set(chunks)),
        "replication_factor": REPLICATION,
        "chunking": chunking,
        "tags": tags
    }


def get_object_row(bucket: str, key: str):
    with db() as conn:
        row = conn.execute("""
            SELECT size, version, chunk_hashes, created_at, tags, chunking
            FROM objects WHERE bucket=%s AND key=%s
        """, (bucket, key)).fetchone()

    if not row:
        raise HTTPException(404, "Object not found")

    return row


@app.get(
    "/metadata/{bucket}/{key:path}",
    dependencies=[Depends(require_key)]
)
def object_metadata(bucket: str, key: str):
    size, version, chunks, created_at, tags, chunking = get_object_row(bucket, key)

    return {
        "bucket": bucket,
        "key": key,
        "size": size,
        "version": version,
        "chunk_hashes": chunks,
        "chunking": chunking,
        "created_at": created_at.isoformat(),
        "tags": tags
    }


@app.get(
    "/objects/{bucket}/{key:path}",
    dependencies=[Depends(require_key)]
)
def download_object(bucket: str, key: str, request: Request):
    size, version, chunks, created_at, tags, chunking = get_object_row(bucket, key)

    range_header = request.headers.get("range")
    start = 0
    end = size - 1
    status_code = 200
    headers = {"Accept-Ranges": "bytes"}

    if range_header:
        try:
            if not range_header.startswith("bytes=") or "," in range_header:
                raise ValueError()
            first, last = range_header[6:].split("-", 1)
            if first:
                start = int(first)
                end = int(last) if last else size - 1
            else:
                suffix_length = int(last)
                if suffix_length <= 0:
                    raise ValueError()
                start = max(0, size - suffix_length)
            if start < 0 or start >= size or end < start:
                raise ValueError()
            end = min(end, size - 1)
        except (ValueError, TypeError):
            raise HTTPException(416, "Invalid byte range")
        status_code = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    headers["Content-Length"] = str(max(0, end - start + 1))

    # Streaming avoids holding the entire reconstructed object in RAM.
    # Chunk failures after streaming begins will terminate the transfer.
    def generate():
        offset = 0
        for chunk_hash in chunks:
            chunk = read_chunk(chunk_hash)
            chunk_end = offset + len(chunk) - 1
            if chunk_end >= start and offset <= end:
                lo = max(0, start - offset)
                hi = min(len(chunk), end - offset + 1)
                yield chunk[lo:hi]
            offset += len(chunk)
            if offset > end:
                break

    return StreamingResponse(
        generate(),
        status_code=status_code,
        headers=headers,
        media_type="application/octet-stream"
    )

@app.get(
    "/buckets/{bucket}/objects",
    dependencies=[Depends(require_key)]
)
def list_objects(bucket: str):
    with db() as conn:
        rows = conn.execute("""
            SELECT key, size, version FROM objects
            WHERE bucket=%s ORDER BY key
        """, (bucket,)).fetchall()

    return {
        "objects": [
            {"key": key, "size": size, "version": version}
            for key, size, version in rows
        ]
    }


@app.get("/search", dependencies=[Depends(require_key)])
def search_objects(
    prefix: str = "",
    min_size: int = 0,
    max_size: int | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    tag_key: str | None = None,
    tag_value: str | None = None
):
    if (tag_key is None) != (tag_value is None):
        raise HTTPException(400, "Provide both tag_key and tag_value")
    if min_size < 0 or (max_size is not None and max_size < min_size):
        raise HTTPException(400, "Invalid size range")
    with db() as conn:
        rows = conn.execute("""
            SELECT bucket, key, size, created_at, tags FROM objects
            WHERE key LIKE %s ESCAPE '\\'
              AND size >= %s
              AND (%s::BIGINT IS NULL OR size <= %s::BIGINT)
              AND (%s::TIMESTAMPTZ IS NULL OR created_at >= %s::TIMESTAMPTZ)
              AND (%s::TIMESTAMPTZ IS NULL OR created_at <= %s::TIMESTAMPTZ)
              AND (%s::TEXT IS NULL OR tags ->> %s::TEXT = %s::TEXT)
            ORDER BY bucket, key
        """, (
            prefix.replace("%", r"\%").replace("_", r"\_") + "%",
            min_size, max_size, max_size,
            created_after, created_after, created_before, created_before,
            tag_key, tag_key, tag_value
        )).fetchall()
    return {"results": [
        {"bucket": b, "key": k, "size": size,
         "created_at": date.isoformat(), "tags": tags}
        for b, k, size, date, tags in rows
    ]}


@app.post("/search/elasticsearch/sync", dependencies=[Depends(require_key)])
def synchronize_elasticsearch():
    """Rebuild the index from PostgreSQL; run with writes paused for consistency."""
    try:
        es_index.ensure_index()
        # Remove stale index documents as well as adding existing metadata.
        with httpx.Client(timeout=30) as client:
            response = client.post(
                f"{es_index.ES_URL}/{es_index.INDEX}/_delete_by_query",
                params={"refresh": "true", "conflicts": "proceed", "wait_for_completion": "true"},
                json={"query": {"match_all": {}}},
            )
            response.raise_for_status()
        with db() as conn:
            rows = conn.execute(
                "SELECT bucket, key, size, version, created_at, tags FROM objects ORDER BY bucket, key"
            ).fetchall()
        for bucket, key, size, version, created_at, tags in rows:
            es_index.index_object(bucket, key, size, version, created_at, tags)
        with httpx.Client(timeout=15) as client:
            response = client.post(f"{es_index.ES_URL}/{es_index.INDEX}/_refresh")
            response.raise_for_status()
        return {"status": "synchronized", "indexed_objects": len(rows), "index": es_index.INDEX}
    except httpx.HTTPError as exc:
        raise HTTPException(503, f"Elasticsearch unavailable: {exc}") from exc


@app.get("/search/elasticsearch", dependencies=[Depends(require_key)])
def search_objects_elasticsearch(
    prefix: str = "", min_size: int = 0, max_size: int | None = None,
    created_after: datetime | None = None, created_before: datetime | None = None,
    tag_key: str | None = None, tag_value: str | None = None
):
    if (tag_key is None) != (tag_value is None):
        raise HTTPException(400, "Provide both tag_key and tag_value")
    if min_size < 0 or (max_size is not None and max_size < min_size):
        raise HTTPException(400, "Invalid size range")
    try:
        results = es_index.search(prefix, min_size, max_size, created_after,
                                  created_before, tag_key, tag_value)
        return {"results": results, "engine": "elasticsearch", "limit": 1000}
    except httpx.HTTPError as exc:
        raise HTTPException(503, f"Elasticsearch unavailable: {exc}") from exc


@app.delete(
    "/objects/{bucket}/{key:path}",
    dependencies=[Depends(require_key)]
)
def delete_object(bucket: str, key: str):
    with db() as conn:
        row = conn.execute("""
            SELECT chunk_hashes FROM objects
            WHERE bucket=%s AND key=%s FOR UPDATE
        """, (bucket, key)).fetchone()

        if not row:
            raise HTTPException(404, "Object not found")

        for chunk_hash in row[0]:
            conn.execute("""
                UPDATE chunks SET ref_count=ref_count-1
                WHERE hash=%s
            """, (chunk_hash,))

        conn.execute(
            "DELETE FROM objects WHERE bucket=%s AND key=%s",
            (bucket, key)
        )

    remove_object_from_elasticsearch(bucket, key)
    return {"status": "metadata_deleted"}


# Prototype S3-style multipart sessions. Parts live on the gateway's local
# disk; use shared durable storage before scaling to multiple gateways.
@app.post("/multipart/{bucket}/{key:path}/initiate", dependencies=[Depends(require_key)])
def initiate_multipart(bucket: str, key: str):
    with db() as conn:
        if not conn.execute("SELECT 1 FROM buckets WHERE name=%s", (bucket,)).fetchone():
            raise HTTPException(404, "Bucket not found")
    upload_id = uuid.uuid4().hex
    folder = UPLOAD_ROOT / upload_id
    folder.mkdir(parents=True)
    (folder / "session.json").write_text(json.dumps({"bucket": bucket, "key": key}))
    return {"upload_id": upload_id, "status": "initiated"}


def multipart_folder(upload_id: str, bucket: str, key: str):
    if len(upload_id) != 32 or any(c not in "0123456789abcdef" for c in upload_id):
        raise HTTPException(400, "Invalid upload ID")
    folder = UPLOAD_ROOT / upload_id
    try:
        session = json.loads((folder / "session.json").read_text())
    except (FileNotFoundError, ValueError):
        raise HTTPException(404, "Multipart session not found")
    if session != {"bucket": bucket, "key": key}:
        raise HTTPException(404, "Multipart session not found")
    return folder


@app.put("/multipart/{bucket}/{key:path}/parts/{part_number}", dependencies=[Depends(require_key)])
async def upload_part(bucket: str, key: str, part_number: int, upload_id: str, request: Request):
    if not 1 <= part_number <= 10000:
        raise HTTPException(400, "Invalid part number")
    folder = multipart_folder(upload_id, bucket, key)
    destination = folder / f"part-{part_number:05d}"
    temp = folder / f"part-{part_number:05d}.tmp"
    digest = hashlib.sha256()
    size = 0
    with temp.open("wb") as output:
        async for block in request.stream():
            output.write(block)
            digest.update(block)
            size += len(block)
    os.replace(temp, destination)
    return {"part_number": part_number, "size": size, "sha256": digest.hexdigest()}


@app.post("/multipart/{bucket}/{key:path}/complete", dependencies=[Depends(require_key)])
async def complete_multipart(bucket: str, key: str, upload_id: str, tags_json: str = "{}", chunking: str = "fixed"):
    folder = multipart_folder(upload_id, bucket, key)
    parts = sorted(folder.glob("part-[0-9][0-9][0-9][0-9][0-9]"))
    if not parts:
        raise HTTPException(400, "No uploaded parts")
    numbers = [int(p.name.split("-")[1]) for p in parts]
    if numbers != list(range(1, len(numbers) + 1)):
        raise HTTPException(400, "Multipart parts must be consecutive from 1")
    # Spooled file limits RAM use while combining uploaded parts.
    with tempfile.SpooledTemporaryFile(max_size=4 * 1024 * 1024, mode="w+b") as combined:
        for part in parts:
            with part.open("rb") as source:
                shutil.copyfileobj(source, combined, length=1024 * 1024)
        combined.seek(0)
        upload = UploadFile(file=combined, filename=key)
        result = await store_object(bucket, key, upload, tags_json, chunking)
    shutil.rmtree(folder)
    return {"status": "completed", "upload_id": upload_id, **result}


@app.delete("/multipart/{bucket}/{key:path}", dependencies=[Depends(require_key)])
def abort_multipart(bucket: str, key: str, upload_id: str):
    folder = multipart_folder(upload_id, bucket, key)
    shutil.rmtree(folder)
    return {"status": "aborted"}


@app.post("/cluster/repair", dependencies=[Depends(require_key)])
def repair_cluster():
    healthy = active_nodes()

    if len(healthy) < REPLICATION:
        raise HTTPException(
            503, "At least three healthy nodes required for repair"
        )

    with db() as conn:
        hashes = [
            row[0] for row in conn.execute(
                "SELECT hash FROM chunks WHERE ref_count > 0"
            ).fetchall()
        ]

    repaired = 0
    failed = 0

    with httpx.Client(timeout=30) as client:
        for chunk_hash in hashes:
            targets = placement(chunk_hash, healthy)

            # Find a verified source among all healthy nodes.
            source_data = None
            existing = set()

            for node in healthy:
                try:
                    response = client.get(
                        f"{NODES[node]}/chunks/{chunk_hash}"
                    )
                    if (
                        response.status_code == 200
                        and sha256(response.content) == chunk_hash
                    ):
                        existing.add(node)
                        source_data = response.content
                except httpx.HTTPError:
                    continue

            if source_data is None:
                failed += 1
                continue

            for node in targets:
                if node in existing:
                    continue

                try:
                    response = client.put(
                        f"{NODES[node]}/chunks/{chunk_hash}",
                        content=source_data
                    )
                    if response.status_code == 200:
                        repaired += 1
                    else:
                        failed += 1
                except httpx.HTTPError:
                    failed += 1

    return {
        "repaired_replicas": repaired,
        "failed_repairs": failed,
        "healthy_nodes": len(healthy)
    }

def automatic_repair_loop():
    """
    Periodically repair missing replicas.
    """
    while True:
        time.sleep(30)

        try:
            healthy = active_nodes()

            if len(healthy) < REPLICATION:
                print(
                    "[AUTO REPAIR] Waiting for at least "
                    "3 healthy storage nodes",
                    flush=True
                )
                continue

            result = repair_cluster()

            print(
                "[AUTO REPAIR] "
                f"Repaired: {result['repaired_replicas']}, "
                f"Failed: {result['failed_repairs']}",
                flush=True
            )

        except Exception as exc:
            print(
                f"[AUTO REPAIR] Error: {exc}",
                flush=True
            )




def scrub_cluster():
    """Verify SHA-256 integrity of expected replicas of referenced chunks."""
    with db() as conn:
        hashes = [row[0] for row in conn.execute(
            "SELECT hash FROM chunks WHERE ref_count > 0"
        ).fetchall()]

    healthy = active_nodes()
    verified = corrupted = missing = errors = 0

    with httpx.Client(timeout=15) as client:
        for chunk_hash in hashes:
            for node in placement(chunk_hash, healthy):
                try:
                    response = client.head(f"{NODES[node]}/chunks/{chunk_hash}")
                    if response.status_code == 200:
                        verified += 1
                    elif response.status_code == 409:
                        corrupted += 1
                    elif response.status_code == 404:
                        missing += 1
                    else:
                        errors += 1
                except httpx.HTTPError:
                    errors += 1

    return {
        "verified_replicas": verified,
        "corrupted_replicas": corrupted,
        "missing_replicas": missing,
        "errors": errors,
        "healthy_nodes": len(healthy),
    }


def background_scrubber_loop():
    """Run integrity checks every 60 seconds."""
    while True:
        time.sleep(60)
        try:
            result = scrub_cluster()
            print(
                "[SCRUBBER] "
                f"Verified: {result['verified_replicas']}, "
                f"Corrupted: {result['corrupted_replicas']}, "
                f"Missing: {result['missing_replicas']}, "
                f"Errors: {result['errors']}",
                flush=True,
            )
        except Exception as exc:
            print(f"[SCRUBBER] Error: {exc}", flush=True)
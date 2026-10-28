
import hashlib
import os
import shutil
import threading
import time
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response

app = FastAPI(title="Distributed Storage Node")

NODE_ID = os.getenv("NODE_ID", "storage-test")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:8000")

DATA_DIR.mkdir(parents=True, exist_ok=True)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def chunk_path(chunk_hash: str) -> Path:
    if len(chunk_hash) != 64 or any(c not in "0123456789abcdef" for c in chunk_hash):
        raise HTTPException(400, "Invalid SHA-256 hash")
    return DATA_DIR / chunk_hash


@app.get("/health")
def health():
    return {"node": NODE_ID, "status": "healthy"}


@app.get("/stats")
def stats():
    usage = shutil.disk_usage(DATA_DIR)
    count = sum(1 for p in DATA_DIR.iterdir() if p.is_file())
    return {
        "node_id": NODE_ID,
        "free_space": usage.free,
        "chunk_count": count
    }


@app.put("/chunks/{chunk_hash}")
async def put_chunk(chunk_hash: str, request: Request):
    path = chunk_path(chunk_hash)
    data = await request.body()

    if sha256(data) != chunk_hash:
        raise HTTPException(400, "SHA-256 mismatch")

    if path.exists():
        if sha256(path.read_bytes()) == chunk_hash:
            return {
                "status": "already_exists",
                "node": NODE_ID,
                "hash": chunk_hash
            }

    temp_path = DATA_DIR / f"{chunk_hash}.tmp"
    temp_path.write_bytes(data)
    os.replace(temp_path, path)

    return {"status": "stored", "node": NODE_ID, "hash": chunk_hash}


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    path = chunk_path(chunk_hash)

    if not path.exists():
        raise HTTPException(404, "Chunk not found")

    data = path.read_bytes()

    if sha256(data) != chunk_hash:
        raise HTTPException(409, "Stored chunk is corrupted")

    return Response(
        content=data,
        media_type="application/octet-stream"
    )


@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str):
    path = chunk_path(chunk_hash)

    if not path.exists():
        raise HTTPException(404, "Chunk not found")

    data = path.read_bytes()

    if sha256(data) != chunk_hash:
        raise HTTPException(409, "Stored chunk is corrupted")

    return Response(
        status_code=200,
        headers={"Content-Length": str(len(data))}
    )


@app.delete("/chunks/{chunk_hash}")
def delete_chunk(chunk_hash: str):
    path = chunk_path(chunk_hash)

    if not path.exists():
        raise HTTPException(404, "Chunk not found")

    path.unlink()
    return {"status": "deleted", "node": NODE_ID}


def heartbeat_loop():
    while True:
        try:
            information = stats()
            with httpx.Client(timeout=3) as client:
                client.post(
                    f"{GATEWAY_URL}/internal/heartbeat",
                    json=information
                )
        except Exception as exc:
            print(f"Heartbeat failed: {exc}", flush=True)

        time.sleep(5)


@app.on_event("startup")
def start_heartbeat():
    threading.Thread(
        target=heartbeat_loop,
        daemon=True
    ).start()

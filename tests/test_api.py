
import hashlib
import random
import sys
import uuid
from pathlib import Path

import requests

# Import Phase 3 chunking functions
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "gateway"))

from chunking import (
    fixed_chunking,
    content_defined_chunking,
    chunk_hashes
)

BASE_URL = "http://localhost:8000"
HEADERS = {"X-API-Key": "midterm-secret"}


# =========================================================
# PHASE 5 - EXISTING API TESTS
# =========================================================

def test_cluster_has_four_nodes():
    response = requests.get(
        f"{BASE_URL}/cluster/status",
        headers=HEADERS,
        timeout=10
    )
    assert response.status_code == 200
    assert response.json()["healthy_nodes"] == 4


def test_api_key_required():
    response = requests.get(
        f"{BASE_URL}/cluster/status",
        timeout=10
    )
    assert response.status_code == 401


def test_upload_download_and_metadata():
    bucket = "pytest-bucket"
    key = f"test-{uuid.uuid4().hex}.bin"
    content = b"Distributed Storage Test" * 100000

    response = requests.post(
        f"{BASE_URL}/buckets/{bucket}",
        headers=HEADERS,
        timeout=10
    )
    assert response.status_code == 200

    response = requests.put(
        f"{BASE_URL}/objects/{bucket}/{key}",
        headers=HEADERS,
        files={"file": (key, content)},
        timeout=120
    )
    assert response.status_code == 200
    assert response.json()["chunks"] >= 2

    response = requests.get(
        f"{BASE_URL}/metadata/{bucket}/{key}",
        headers=HEADERS,
        timeout=10
    )
    assert response.status_code == 200
    assert len(response.json()["chunk_hashes"]) >= 2

    response = requests.get(
        f"{BASE_URL}/objects/{bucket}/{key}",
        headers=HEADERS,
        timeout=120
    )
    assert response.status_code == 200

    assert hashlib.sha256(response.content).digest() == (
        hashlib.sha256(content).digest()
    )

    response = requests.get(
        f"{BASE_URL}/search",
        headers=HEADERS,
        params={"prefix": key},
        timeout=10
    )
    assert response.status_code == 200
    assert any(
        item["key"] == key
        for item in response.json()["results"]
    )

    response = requests.delete(
        f"{BASE_URL}/objects/{bucket}/{key}",
        headers=HEADERS,
        timeout=10
    )
    assert response.status_code == 200


def test_byte_range():
    bucket = "pytest-bucket"
    key = f"range-{uuid.uuid4().hex}.txt"
    content = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    requests.post(
        f"{BASE_URL}/buckets/{bucket}",
        headers=HEADERS,
        timeout=10
    )

    upload = requests.put(
        f"{BASE_URL}/objects/{bucket}/{key}",
        headers=HEADERS,
        files={"file": (key, content)},
        timeout=30
    )
    assert upload.status_code == 200

    response = requests.get(
        f"{BASE_URL}/objects/{bucket}/{key}",
        headers={**HEADERS, "Range": "bytes=0-9"},
        timeout=30
    )

    assert response.status_code == 206
    assert response.content == b"0123456789"

    requests.delete(
        f"{BASE_URL}/objects/{bucket}/{key}",
        headers=HEADERS,
        timeout=10
    )


# =========================================================
# PHASE 3 - CONTENT-DEFINED CHUNKING TESTS
# =========================================================

def test_fixed_chunking_reconstruction():
    data = b"ABC123" * 500000

    chunks = fixed_chunking(data)

    assert len(chunks) > 1
    assert b"".join(chunks) == data

    print("\nFixed-size reconstruction: PASS")
    print("Number of chunks:", len(chunks))
    print(
        "SHA-256 verified:",
        hashlib.sha256(b"".join(chunks)).hexdigest()
        == hashlib.sha256(data).hexdigest()
    )



def test_cdc_reconstruction():
    rng = random.Random(42)
    data = rng.randbytes(8 * 1024 * 1024)

    chunks = content_defined_chunking(data)

    assert len(chunks) > 1
    assert b"".join(chunks) == data
    assert all(
        len(chunk) <= 4 * 1024 * 1024
        for chunk in chunks
    )

    print("\nCDC reconstruction: PASS")
    print("Number of chunks:", len(chunks))
    print(
        "SHA-256 verified:",
        hashlib.sha256(b"".join(chunks)).hexdigest()
        == hashlib.sha256(data).hexdigest()
    )



def test_chunk_reuse_after_insertion():
    rng = random.Random(42)

    original = rng.randbytes(8 * 1024 * 1024)
    modified = b"INSERTED-DATA" * 100 + original

    fixed_original = fixed_chunking(original)
    fixed_modified = fixed_chunking(modified)

    cdc_original = content_defined_chunking(original)
    cdc_modified = content_defined_chunking(modified)

    fixed_reused = len(
        set(chunk_hashes(fixed_original))
        & set(chunk_hashes(fixed_modified))
    )

    cdc_reused = len(
        set(chunk_hashes(cdc_original))
        & set(chunk_hashes(cdc_modified))
    )

    assert b"".join(cdc_modified) == modified
    assert cdc_reused > fixed_reused

    original_hash = hashlib.sha256(modified).hexdigest()
    reconstructed_hash = hashlib.sha256(
        b"".join(cdc_modified)
    ).hexdigest()

    assert original_hash == reconstructed_hash

    print("\n========== CHUNK REUSE COMPARISON ==========")
    print("Original size:", len(original), "bytes")
    print("Modified size:", len(modified), "bytes")
    print("Fixed-size chunks:", len(fixed_modified))
    print("CDC chunks:", len(cdc_modified))
    print("Fixed-size reused chunks:", fixed_reused)
    print("CDC reused chunks:", cdc_reused)
    print("CDC improves reuse:", cdc_reused > fixed_reused)
    print("SHA-256 verified:", original_hash == reconstructed_hash)
    print("============================================")

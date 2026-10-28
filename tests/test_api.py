
import hashlib
import uuid

import requests

BASE_URL = "http://localhost:8000"
HEADERS = {"X-API-Key": "midterm-secret"}


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

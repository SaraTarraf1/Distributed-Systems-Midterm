import hashlib
import random
import uuid

import requests

BASE_URL = "http://localhost:8000"
HEADERS = {"X-API-Key": "midterm-secret"}


def test_cdc_distributed_upload_download():
    rng = random.Random(42)
    content = rng.randbytes(8 * 1024 * 1024)
    bucket = "cdc-test-bucket"
    key = f"cdc-{uuid.uuid4().hex}.bin"

    create = requests.post(f"{BASE_URL}/buckets/{bucket}", headers=HEADERS, timeout=15)
    assert create.status_code == 200, create.text

    upload = requests.put(
        f"{BASE_URL}/objects/{bucket}/{key}",
        headers=HEADERS,
        params={"chunking": "cdc"},
        files={"file": (key, content)},
        timeout=180,
    )
    assert upload.status_code == 200, upload.text
    assert upload.json()["chunking"] == "cdc"
    assert upload.json()["chunks"] > 1
    print("\nCDC UPLOAD:", upload.status_code, "chunks:", upload.json()["chunks"])

    metadata = requests.get(
        f"{BASE_URL}/metadata/{bucket}/{key}", headers=HEADERS, timeout=15
    )
    assert metadata.status_code == 200, metadata.text
    assert metadata.json()["chunking"] == "cdc"
    print("CDC METADATA:", metadata.json()["chunking"])

    download = requests.get(
        f"{BASE_URL}/objects/{bucket}/{key}", headers=HEADERS, timeout=180
    )
    assert download.status_code == 200, download.text
    matching = hashlib.sha256(content).digest() == hashlib.sha256(download.content).digest()
    assert matching
    print("DOWNLOAD:", download.status_code, "SHA-256 matching:", matching)

    status = requests.get(f"{BASE_URL}/cluster/status", headers=HEADERS, timeout=15)
    assert status.status_code == 200
    assert status.json()["healthy_nodes"] == 4
    print("Healthy nodes:", status.json()["healthy_nodes"])

    delete = requests.delete(
        f"{BASE_URL}/objects/{bucket}/{key}", headers=HEADERS, timeout=30
    )
    assert delete.status_code == 200, delete.text

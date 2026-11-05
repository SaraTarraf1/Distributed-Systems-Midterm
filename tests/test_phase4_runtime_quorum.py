
import hashlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "gateway"))

import app as gateway


NODES = ["storage1", "storage2", "storage3"]


def make_client(statuses, method):
    client = MagicMock()
    client.__enter__.return_value = client
    client.__exit__.return_value = False

    responses = []
    for status in statuses:
        response = MagicMock()
        response.status_code = status
        responses.append(response)

    getattr(client, method).side_effect = responses
    return client


def test_write_quorum_success():
    data = b"Phase 4 write quorum test"
    chunk_hash = hashlib.sha256(data).hexdigest()

    client = make_client([200, 200, 503], "put")

    with patch.object(gateway, "placement", return_value=NODES), \
         patch.object(gateway.httpx, "Client", return_value=client):

        result = gateway.write_chunk(chunk_hash, data)

    assert result == ["storage1", "storage2"]
    print("WRITE: 2 successful replicas -> PASSED")


def test_write_quorum_failure():
    data = b"Phase 4 failed write test"
    chunk_hash = hashlib.sha256(data).hexdigest()

    client = make_client([200, 503, 503], "put")

    with patch.object(gateway, "placement", return_value=NODES), \
         patch.object(gateway.httpx, "Client", return_value=client):

        with pytest.raises(HTTPException) as error:
            gateway.write_chunk(chunk_hash, data)

    assert error.value.status_code == 503
    print("WRITE: only 1 successful replica -> HTTP 503")


def test_read_quorum_success():
    data = b"Phase 4 read quorum test"
    chunk_hash = hashlib.sha256(data).hexdigest()

    client = MagicMock()
    client.__enter__.return_value = client
    client.__exit__.return_value = False

    responses = []
    for status in [200, 200]:
        response = MagicMock()
        response.status_code = status
        response.content = data
        responses.append(response)

    client.get.side_effect = responses

    with patch.object(gateway, "placement", return_value=NODES), \
         patch.object(gateway, "active_nodes", return_value=NODES), \
         patch.object(gateway.httpx, "Client", return_value=client):

        result = gateway.read_chunk(chunk_hash)

    assert result == data
    print("READ: 2 valid replicas -> PASSED")


def test_read_quorum_failure():
    data = b"Phase 4 failed read test"
    chunk_hash = hashlib.sha256(data).hexdigest()

    client = MagicMock()
    client.__enter__.return_value = client
    client.__exit__.return_value = False

    responses = []

    for status in [200, 404, 404]:
        response = MagicMock()
        response.status_code = status
        response.content = data if status == 200 else b""
        responses.append(response)

    client.get.side_effect = responses

    with patch.object(gateway, "placement", return_value=NODES), \
         patch.object(gateway, "active_nodes", return_value=NODES), \
         patch.object(gateway.httpx, "Client", return_value=client):

        with pytest.raises(HTTPException) as error:
            gateway.read_chunk(chunk_hash)

    assert error.value.status_code == 503
    print("READ: only 1 valid replica -> HTTP 503")

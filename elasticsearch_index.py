"""Elasticsearch metadata index; PostgreSQL remains authoritative."""
import hashlib
import os
import httpx

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200").rstrip("/")
INDEX = "storage-objects-v1"


def document_id(bucket, key):
    return hashlib.sha256(f"{bucket}\0{key}".encode("utf-8")).hexdigest()


def ensure_index():
    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"properties": {
            "bucket": {"type": "keyword"},
            "key": {"type": "keyword"},
            "size": {"type": "long"},
            "version": {"type": "integer"},
            "created_at": {"type": "date"},
            "tags": {"type": "flattened"}
        }}
    }
    with httpx.Client(timeout=15) as client:
        response = client.put(f"{ES_URL}/{INDEX}", json=mapping)
        if response.status_code == 400 and "resource_already_exists_exception" in response.text:
            return
        response.raise_for_status()


def index_object(bucket, key, size, version, created_at, tags):
    doc = {"bucket": bucket, "key": key, "size": size, "version": version,
           "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else created_at,
           "tags": tags or {}}
    with httpx.Client(timeout=15) as client:
        response = client.put(f"{ES_URL}/{INDEX}/_doc/{document_id(bucket, key)}", json=doc)
        response.raise_for_status()


def remove_object(bucket, key):
    with httpx.Client(timeout=15) as client:
        response = client.delete(f"{ES_URL}/{INDEX}/_doc/{document_id(bucket, key)}")
        if response.status_code != 404:
            response.raise_for_status()


def search(prefix="", min_size=0, max_size=None, created_after=None, created_before=None,
           tag_key=None, tag_value=None):
    filters = [{"range": {"size": {"gte": min_size}}}]
    if prefix:
        filters.append({"prefix": {"key": {"value": prefix}}})
    if max_size is not None:
        filters.append({"range": {"size": {"lte": max_size}}})
    dates = {}
    if created_after is not None:
        dates["gte"] = created_after.isoformat()
    if created_before is not None:
        dates["lte"] = created_before.isoformat()
    if dates:
        filters.append({"range": {"created_at": dates}})
    if tag_key is not None:
        # Flattened tags are indexed as keyword-like values.
        filters.append({"term": {f"tags.{tag_key}": tag_value}})
    query = {"size": 1000, "sort": [{"bucket": "asc"}, {"key": "asc"}],
             "query": {"bool": {"filter": filters}}}
    with httpx.Client(timeout=15) as client:
        response = client.post(f"{ES_URL}/{INDEX}/_search", json=query)
        response.raise_for_status()
        return [hit["_source"] for hit in response.json()["hits"]["hits"]]

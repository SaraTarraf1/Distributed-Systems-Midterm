import time

from fastapi import Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

REQUESTS = Counter(
    "storage_http_requests_total",
    "Total gateway HTTP requests",
    ["method", "endpoint", "status"],
)

LATENCY = Histogram(
    "storage_http_request_duration_seconds",
    "Gateway HTTP request latency",
    ["method", "endpoint"],
)

DISK_FREE = Gauge(
    "storage_node_free_bytes",
    "Free disk space reported by each storage node",
    ["node"],
)

DEGRADED = Gauge(
    "storage_degraded_objects",
    "Number of degraded objects in the most recent scan",
)

REPAIRS = Counter(
    "storage_repaired_replicas_total",
    "Total successfully repaired chunk replicas",
)


async def metrics_middleware(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)

    start = time.perf_counter()

    try:
        response = await call_next(request)
    except Exception:
        endpoint = request.scope.get("route")
        path = getattr(endpoint, "path", "unmatched")

        REQUESTS.labels(
            request.method, path, "500"
        ).inc()

        LATENCY.labels(
            request.method, path
        ).observe(time.perf_counter() - start)

        raise

    endpoint = request.scope.get("route")
    path = getattr(endpoint, "path", "unmatched")

    REQUESTS.labels(
        request.method, path, str(response.status_code)
    ).inc()

    LATENCY.labels(
        request.method, path
    ).observe(time.perf_counter() - start)

    return response


def metrics_response():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


def update_disk_metrics(nodes):
    for node in nodes:
        if node["free_space"] is not None:
            DISK_FREE.labels(node["id"]).set(node["free_space"])


def update_degraded_metrics(count):
    DEGRADED.set(count)


def record_repairs(count):
    if count > 0:
        REPAIRS.inc(count)
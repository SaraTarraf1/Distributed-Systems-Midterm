
import hashlib

MIN_CHUNK_SIZE = 256 * 1024
AVG_CHUNK_SIZE = 1024 * 1024
MAX_CHUNK_SIZE = 4 * 1024 * 1024

WINDOW_SIZE = 64
BASE = 257
MOD = 2**64


def fixed_chunking(data, chunk_size=AVG_CHUNK_SIZE):
    """Split data into fixed-size chunks."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    return [
        data[i:i + chunk_size]
        for i in range(0, len(data), chunk_size)
    ]


def content_defined_chunking(
    data,
    min_size=MIN_CHUNK_SIZE,
    avg_size=AVG_CHUNK_SIZE,
    max_size=MAX_CHUNK_SIZE
):
    """Split data using a rolling hash to identify boundaries."""

    if not (0 < min_size <= avg_size <= max_size):
        raise ValueError("Invalid chunk size configuration")

    if not data:
        return []

    mask = (1 << (avg_size.bit_length() - 1)) - 1

    chunks = []
    start = 0
    rolling_hash = 0
    window = bytearray()
    base_power = pow(BASE, WINDOW_SIZE - 1, MOD)

    for i, byte in enumerate(data):
        if len(window) == WINDOW_SIZE:
            old = window.pop(0)
            rolling_hash = (
                rolling_hash - old * base_power
            ) % MOD

        window.append(byte)
        rolling_hash = (
            rolling_hash * BASE + byte
        ) % MOD

        current_size = i - start + 1

        forced = current_size >= max_size

        if current_size < min_size and not forced:
            continue

        boundary = (rolling_hash & mask) == 0

        if boundary or forced:
            chunks.append(data[start:i + 1])
            start = i + 1

            rolling_hash = 0
            window.clear()

    if start < len(data):
        chunks.append(data[start:])

    return chunks


def chunk_hashes(chunks):
    return [
        hashlib.sha256(chunk).hexdigest()
        for chunk in chunks
    ]

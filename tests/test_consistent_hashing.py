
import hashlib
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "gateway"))

from app import placement


def test_consistent_hashing_movement():
    hashes = [
        hashlib.sha256(f"chunk-{i}".encode()).hexdigest()
        for i in range(10000)
    ]

    before = ["storage1", "storage2", "storage3", "storage4"]
    after_removal = ["storage1", "storage2", "storage3"]
    after_addition = before + ["storage5"]

    removal_moved = sum(
        placement(h, before, count=1)[0]
        != placement(h, after_removal, count=1)[0]
        for h in hashes
    )

    addition_moved = sum(
        placement(h, before, count=1)[0]
        != placement(h, after_addition, count=1)[0]
        for h in hashes
    )

    removal_percentage = removal_moved / len(hashes) * 100
    addition_percentage = addition_moved / len(hashes) * 100

    print("\nCONSISTENT HASHING MOVEMENT")
    print("Total simulated chunks:", len(hashes))
    print("Nodes before:", before)
    print("Nodes after removal:", after_removal)
    print("Movement after removal:", round(removal_percentage, 2), "%")
    print("Nodes after addition:", after_addition)
    print("Movement after addition:", round(addition_percentage, 2), "%")

    assert 0 < removal_percentage < 50
    assert 0 < addition_percentage < 50

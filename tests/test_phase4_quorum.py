
import ast
from pathlib import Path

GATEWAY_FILE = (
    Path(__file__).resolve().parents[1]
    / "gateway"
    / "app.py"
)


def load_gateway_configuration():
    source = GATEWAY_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source)

    settings = {}

    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    if target.id in (
                        "REPLICATION",
                        "WRITE_QUORUM",
                        "READ_QUORUM",
                    ):
                        settings[target.id] = ast.literal_eval(
                            node.value
                        )

    return settings


def test_replication_factor():
    settings = load_gateway_configuration()

    assert settings["REPLICATION"] == 3
    print("Replication factor N = 3: PASSED")


def test_write_quorum():
    settings = load_gateway_configuration()

    assert settings["WRITE_QUORUM"] == 2
    assert settings["WRITE_QUORUM"] <= settings["REPLICATION"]

    print("Write quorum W = 2: PASSED")


def test_read_quorum():
    settings = load_gateway_configuration()

    assert settings["READ_QUORUM"] == 2
    assert settings["READ_QUORUM"] <= settings["REPLICATION"]

    print("Read quorum R = 2: PASSED")


def test_quorum_intersection():
    settings = load_gateway_configuration()

    n = settings["REPLICATION"]
    w = settings["WRITE_QUORUM"]
    r = settings["READ_QUORUM"]

    assert w + r > n

    print("Quorum intersection W + R > N: PASSED")

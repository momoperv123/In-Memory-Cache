from __future__ import annotations

import os
import socket
import sys
from typing import Generator

import pytest

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from redis_clone import Client


def is_server_running(host: str = "127.0.0.1", port: int = 31337) -> bool:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except OSError:
        return False


@pytest.fixture
def client() -> Generator[Client, None, None]:
    if not is_server_running():
        pytest.skip("Server is not running. Please start server.py first.")

    return Client()


def test_set_and_get(client: Client) -> None:
    assert client.set("foo", "bar") == 1
    assert client.get("foo") == "bar"


def test_mset_and_mget(client: Client) -> None:
    assert client.mset("a", "1", "b", "2") == 2
    assert client.mget("a", "b") == ["1", "2"]


def test_delete(client: Client) -> None:
    client.set("a", "1")
    assert client.delete("a") == 1
    assert client.get("a") is None


def test_flush(client: Client) -> None:
    client.set("x", "y")
    client.set("z", "w")
    result = client.flush()
    assert result >= 1
    assert client.get("x") is None
    assert client.get("z") is None


def test_command_error(client):
    result = client.execute("BADCOMMAND")
    assert isinstance(result, list)
    assert len(result) == 1
    assert "ERR unknown command BADCOMMAND" in result[0]


def test_arity_errors(client):
    result = client.execute("GET")
    assert isinstance(result, list)
    assert "wrong number of arguments" in result[0]

    result = client.execute("SET", "key")
    assert isinstance(result, list)
    assert "wrong number of arguments" in result[0]


def test_empty_values(client):
    assert client.set("empty", "") == 1
    assert client.get("empty") == ""

    assert client.set("none", "None") == 1
    assert client.get("none") == "None"


def test_large_values(client):
    large_value = "x" * 1000
    assert client.set("large", large_value) == 1
    assert client.get("large") == large_value


def test_special_characters(client):
    special_value = "hello\nworld\twith\rspecial chars"
    assert client.set("special", special_value) == 1
    assert client.get("special") == special_value


def test_unicode_values(client):
    unicode_value = "你好世界 🌍"
    assert client.set("unicode", unicode_value) == 1
    assert client.get("unicode") == unicode_value


def test_numeric_values(client):
    assert client.set("number", "123") == 1
    assert client.get("number") == "123"

    assert client.set("float", "3.14") == 1
    assert client.get("float") == "3.14"


def test_key_overwrite(client):
    assert client.set("overwrite", "first") == 1
    assert client.get("overwrite") == "first"

    assert client.set("overwrite", "second") == 1
    assert client.get("overwrite") == "second"


def test_nonexistent_key_operations(client):
    assert client.get("nonexistent") is None

    assert client.delete("nonexistent") == 0


def test_multiple_operations_sequence(client):
    assert client.set("seq1", "value1") == 1
    assert client.set("seq2", "value2") == 1
    assert client.set("seq3", "value3") == 1

    assert client.get("seq1") == "value1"
    assert client.get("seq2") == "value2"
    assert client.get("seq3") == "value3"

    assert client.delete("seq2") == 1
    assert client.get("seq2") is None

    assert client.get("seq1") == "value1"
    assert client.get("seq3") == "value3"


def test_mget_with_nonexistent_keys(client):
    client.set("exists1", "value1")
    client.set("exists2", "value2")

    result = client.mget("exists1", "nonexistent", "exists2")
    assert result == ["value1", None, "value2"]


def test_mset_with_odd_number_of_args(client):
    # MSET with odd args should still work with available pairs
    result = client.mset("key1", "value1", "key2", "value2", "key3")
    assert result == 2  # Only 2 complete pairs

    assert client.get("key1") == "value1"
    assert client.get("key2") == "value2"
    assert client.get("key3") is None


def test_concurrent_like_operations(client):
    # Set multiple keys quickly
    for i in range(10):
        assert client.set(f"concurrent_{i}", f"value_{i}") == 1

    # Get them all back
    for i in range(10):
        assert client.get(f"concurrent_{i}") == f"value_{i}"

    # Delete some
    for i in range(0, 10, 2):
        assert client.delete(f"concurrent_{i}") == 1

    # Check remaining
    for i in range(10):
        if i % 2 == 0:
            assert client.get(f"concurrent_{i}") is None
        else:
            assert client.get(f"concurrent_{i}") == f"value_{i}"


def test_flush_with_multiple_keys(client):
    # Set multiple keys
    for i in range(5):
        client.set(f"flush_test_{i}", f"value_{i}")

    # Verify they exist
    for i in range(5):
        assert client.get(f"flush_test_{i}") == f"value_{i}"

    # Flush all
    result = client.flush()
    assert result >= 5

    # Verify all are gone
    for i in range(5):
        assert client.get(f"flush_test_{i}") is None


def test_flush_empty_database(client):
    result = client.flush()
    assert result == 0  # No keys to delete


def test_case_sensitive_keys(client):
    assert client.set("Key", "value1") == 1
    assert client.set("key", "value2") == 1
    assert client.set("KEY", "value3") == 1

    assert client.get("Key") == "value1"
    assert client.get("key") == "value2"
    assert client.get("KEY") == "value3"


def test_key_with_spaces(client):
    assert client.set("key with spaces", "value") == 1
    assert client.get("key with spaces") == "value"

    assert client.set("  leading spaces", "value") == 1
    assert client.get("  leading spaces") == "value"


def test_very_long_key(client):
    long_key = "x" * 100
    assert client.set(long_key, "value") == 1
    assert client.get(long_key) == "value"


def test_connection_reuse(client):
    assert client.set("reuse1", "value1") == 1
    assert client.set("reuse2", "value2") == 1
    assert client.get("reuse1") == "value1"
    assert client.get("reuse2") == "value2"
    assert client.delete("reuse1") == 1
    assert client.get("reuse1") is None
    assert client.get("reuse2") == "value2"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

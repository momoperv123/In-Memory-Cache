from __future__ import annotations

import os
import sys
import time
from typing import Generator

import pytest

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from redis_clone import Client


@pytest.fixture
def client() -> Generator[Client, None, None]:
    client = Client()
    # Clear database before each test to ensure isolation
    client.flush()
    return client


def test_expire_command(client: Client) -> None:
    client.set("test_key", "value")

    # Set 1 second expiry
    assert client.execute("EXPIRE", "test_key", 1) == 1

    # Key should still exist
    assert client.get("test_key") == "value"


def test_pexpire_command(client: Client) -> None:
    client.set("test_key", "value")

    # Set 100ms expiry
    assert client.execute("PEXPIRE", "test_key", 100) == 1


def test_ttl_command(client: Client) -> None:
    client.set("test_key", "value")

    # No TTL set
    assert client.execute("TTL", "test_key") == -1

    # Set TTL
    client.execute("EXPIRE", "test_key", 5)
    ttl = client.execute("TTL", "test_key")
    assert 1 <= ttl <= 5


def test_pttl_command(client: Client) -> None:
    client.set("test_key", "value")

    # No TTL set
    assert client.execute("PTTL", "test_key") == -1

    # Set TTL
    client.execute("PEXPIRE", "test_key", 5000)
    pttl = client.execute("PTTL", "test_key")
    assert 1000 <= pttl <= 5000


def test_exists_command(client: Client) -> None:
    client.set("key1", "value1")
    client.set("key2", "value2")

    assert client.execute("EXISTS", "key1") == 1
    assert client.execute("EXISTS", "key1", "key2") == 2
    assert client.execute("EXISTS", "nonexistent") == 0


def test_keys_command(client: Client) -> None:
    client.set("key1", "value1")
    client.set("key2", "value2")

    keys = client.execute("KEYS", "*")
    assert "key1" in keys
    assert "key2" in keys
    assert len(keys) == 2


def test_arity_errors(client: Client) -> None:
    result = client.execute("GET")
    assert isinstance(result, list)
    assert "wrong number of arguments" in result[0]

    result = client.execute("SET", "key")
    assert isinstance(result, list)
    assert "wrong number of arguments" in result[0]


def test_command_errors(client: Client) -> None:
    result = client.execute("BADCOMMAND")
    assert isinstance(result, list)
    assert "unknown command" in result[0]


def test_ttl_nonexistent_key(client: Client) -> None:
    assert client.execute("TTL", "nonexistent_key") == -2
    assert client.execute("PTTL", "nonexistent_key") == -2


def test_expire_nonexistent_key(client: Client) -> None:
    assert client.execute("EXPIRE", "nonexistent_key", 10) == 0
    assert client.execute("PEXPIRE", "nonexistent_key", 10000) == 0


def test_ttl_expiration(client: Client) -> None:
    client.set("expire_key", "value")

    # Set very short TTL
    assert client.execute("EXPIRE", "expire_key", 1) == 1

    # Key should still exist
    assert client.get("expire_key") == "value"

    # TTL should be 1
    ttl = client.execute("TTL", "expire_key")
    assert ttl == 1

    # Wait for expiration
    time.sleep(1.1)

    # Key should be expired
    assert client.get("expire_key") is None
    assert client.execute("TTL", "expire_key") == -2
    assert client.execute("PTTL", "expire_key") == -2


def test_pexpire_expiration(client: Client) -> None:
    client.set("pexpire_key", "value")

    # Set very short TTL (100ms)
    assert client.execute("PEXPIRE", "pexpire_key", 100) == 1

    # Key should still exist
    assert client.get("pexpire_key") == "value"

    # PTTL should be around 100ms
    pttl = client.execute("PTTL", "pexpire_key")
    assert 50 <= pttl <= 100

    # Wait for expiration
    time.sleep(0.15)

    # Key should be expired
    assert client.get("pexpire_key") is None
    assert client.execute("PTTL", "pexpire_key") == -2


def test_ttl_update(client: Client) -> None:
    client.set("update_key", "value")

    # Initially no TTL
    assert client.execute("TTL", "update_key") == -1

    # Set TTL
    assert client.execute("EXPIRE", "update_key", 10) == 1
    ttl = client.execute("TTL", "update_key")
    assert 1 <= ttl <= 10

    # Update TTL
    assert client.execute("EXPIRE", "update_key", 20) == 1
    ttl = client.execute("TTL", "update_key")
    assert 1 <= ttl <= 20


def test_ttl_remove(client: Client) -> None:
    client.set("remove_key", "value")

    # Set TTL
    assert client.execute("EXPIRE", "remove_key", 10) == 1
    assert client.execute("TTL", "remove_key") > 0

    # Remove TTL by setting key again
    client.set("remove_key", "new_value")
    assert client.execute("TTL", "remove_key") == -1


def test_exists_with_ttl(client: Client) -> None:
    client.set("exists_key", "value")

    # Key exists
    assert client.execute("EXISTS", "exists_key") == 1

    # Set TTL
    assert client.execute("EXPIRE", "exists_key", 1) == 1

    # Still exists
    assert client.execute("EXISTS", "exists_key") == 1

    # Wait for expiration
    time.sleep(1.1)

    # No longer exists
    assert client.execute("EXISTS", "exists_key") == 0


def test_keys_with_ttl(client: Client) -> None:
    client.set("key1", "value1")
    client.set("key2", "value2")
    client.set("key3", "value3")

    # All keys exist
    keys = client.execute("KEYS", "*")
    assert len(keys) == 3
    assert "key1" in keys
    assert "key2" in keys
    assert "key3" in keys

    # Set TTL on one key
    assert client.execute("EXPIRE", "key2", 1) == 1

    # All keys still exist
    keys = client.execute("KEYS", "*")
    assert len(keys) == 3

    # Wait for expiration
    time.sleep(1.1)

    # Only 2 keys remain
    keys = client.execute("KEYS", "*")
    assert len(keys) == 2
    assert "key1" in keys
    assert "key3" in keys
    assert "key2" not in keys


def test_ttl_edge_cases(client: Client) -> None:
    client.set("edge_key", "value")

    # Set TTL to 0 (should expire immediately)
    assert client.execute("EXPIRE", "edge_key", 0) == 1
    assert client.get("edge_key") is None
    assert client.execute("TTL", "edge_key") == -2

    # Set negative TTL (should be invalid)
    client.set("edge_key", "value")
    assert client.execute("EXPIRE", "edge_key", -1) == 0
    assert client.get("edge_key") == "value"
    assert client.execute("TTL", "edge_key") == -1


def test_pttl_edge_cases(client: Client) -> None:
    client.set("pttl_key", "value")

    # Set PTTL to 0 (should expire immediately)
    assert client.execute("PEXPIRE", "pttl_key", 0) == 1
    assert client.get("pttl_key") is None
    assert client.execute("PTTL", "pttl_key") == -2

    # Set negative PTTL (should be invalid)
    client.set("pttl_key", "value")
    assert client.execute("PEXPIRE", "pttl_key", -1) == 0
    assert client.get("pttl_key") == "value"
    assert client.execute("PTTL", "pttl_key") == -1

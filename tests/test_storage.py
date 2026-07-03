import time

from kademlia import (
    KEY_EXPIRY_SECONDS,
    MIN_CACHE_TTL_SECONDS,
    KademliaServer,
    StoredValue,
    compute_cache_ttl,
)


def test_stored_value_default_expiry():
    entry = StoredValue("v", is_original_publisher=True)
    assert not entry.is_expired()
    assert entry.expires_at - time.time() <= KEY_EXPIRY_SECONDS + 1


def test_stored_value_explicit_expiry():
    entry = StoredValue("v", is_original_publisher=False, expires_at=time.time() - 1)
    assert entry.is_expired()


def test_cache_ttl_min_bound():
    a = "f" * 40
    b = "0" * 40
    assert compute_cache_ttl(a, b) >= MIN_CACHE_TTL_SECONDS


def test_cache_ttl_shrinks_with_distance():
    own = "0" * 40
    near = "0" * 39 + "f"
    far = "f" * 40
    assert compute_cache_ttl(own, near) >= compute_cache_ttl(own, far)


def test_publisher_flag_survives_remote_overwrite():
    # Regression: a neighbor's republish "set" must not demote the
    # original publisher's entry to a non-publisher one.
    server = KademliaServer()
    server.data_store["k"] = StoredValue("v1", is_original_publisher=True)
    server._store_remote_value("k", "v2")
    entry = server.data_store["k"]
    assert entry.is_original_publisher is True
    assert entry.value == "v2"


def test_remote_store_on_fresh_key_is_not_publisher():
    server = KademliaServer()
    server._store_remote_value("k", "v")
    assert server.data_store["k"].is_original_publisher is False


def test_expired_publisher_entry_does_not_keep_flag():
    server = KademliaServer()
    server.data_store["k"] = StoredValue(
        "v1", is_original_publisher=True, expires_at=time.time() - 1
    )
    server._store_remote_value("k", "v2")
    assert server.data_store["k"].is_original_publisher is False


def test_cached_store_uses_distance_ttl():
    server = KademliaServer()
    target_id = "f" * 40
    server._store_cached_value("k", "v", target_id)
    entry = server.data_store["k"]
    ttl = entry.expires_at - time.time()
    assert MIN_CACHE_TTL_SECONDS - 5 <= ttl <= KEY_EXPIRY_SECONDS

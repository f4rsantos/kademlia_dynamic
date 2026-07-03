import time

from kademlia import (
    KademliaServer,
    Peer,
    StoredValue,
    compute_cache_ttl,
    generate_node_id,
)


def test_custom_k_limits_bucket_capacity():
    server = KademliaServer(k=3)
    bucket = server.routing_table._buckets[0]
    assert bucket.k == 3
    for i in range(3):
        bucket.add_new(Peer(generate_node_id(), "127.0.0.1", 8000 + i))
    assert not bucket.has_capacity()


def test_custom_k_survives_bucket_split():
    server = KademliaServer(k=2)
    table = server.routing_table
    for i in range(6):
        table.try_insert(Peer(generate_node_id(), "127.0.0.1", 8000 + i))
    for bucket in table._buckets:
        assert bucket.k == 2
        assert len(bucket.peers) <= 2


def test_custom_k_caps_find_nearest():
    server = KademliaServer(k=2)
    for i in range(6):
        server.routing_table.try_insert(Peer(generate_node_id(), "127.0.0.1", 8000 + i))
    assert len(server.routing_table.find_nearest(generate_node_id())) <= 2


def test_tunables_stored_on_server():
    server = KademliaServer(
        alpha=5,
        query_timeout=0.5,
        bucket_refresh_interval=10,
        key_expiry_seconds=100,
        non_publisher_restore_interval=20,
        original_publisher_republish_interval=40,
        min_cache_ttl_seconds=5,
        max_dispatch_tasks=8,
    )
    assert server.alpha == 5
    assert server.query_timeout == 0.5
    assert server.bucket_refresh_interval == 10
    assert server.key_expiry_seconds == 100
    assert server.non_publisher_restore_interval == 20
    assert server.original_publisher_republish_interval == 40
    assert server.min_cache_ttl_seconds == 5
    assert server.max_dispatch_tasks == 8
    assert server.routing_table.refresh_interval == 10


def test_custom_key_expiry_applied_to_stored_values():
    server = KademliaServer(key_expiry_seconds=50)
    server._store_remote_value("k", "v")
    ttl = server.data_store["k"].expires_at - time.time()
    assert 45 <= ttl <= 50


def test_custom_cache_ttl_bounds_applied():
    server = KademliaServer(key_expiry_seconds=100, min_cache_ttl_seconds=30)
    server._store_cached_value("k", "v", "f" * 40)
    ttl = server.data_store["k"].expires_at - time.time()
    assert 25 <= ttl <= 100


def test_compute_cache_ttl_custom_params():
    far = "f" * 40
    own = "0" * 40
    assert compute_cache_ttl(own, far, key_expiry_seconds=100, min_cache_ttl_seconds=7) == 7


def test_needs_republish_explicit_interval():
    entry = StoredValue("v", is_original_publisher=True)
    entry.last_republished_at = time.time() - 10
    assert entry.needs_republish(interval=5)
    assert not entry.needs_republish(interval=60)

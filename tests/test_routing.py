import secrets

import pytest

from kademlia import (
    K_BUCKET_SIZE,
    KademliaServer,
    Peer,
    RoutingTable,
    xor_distance,
)


def make_node_id(first_bit: int) -> str:
    raw = bytearray(secrets.token_bytes(20))
    if first_bit:
        raw[0] |= 0x80
    else:
        raw[0] &= 0x7F
    return bytes(raw).hex()


def make_peer(first_bit: int, port: int = 8000) -> Peer:
    return Peer(make_node_id(first_bit), "127.0.0.1", port)


def opposite_first_bit(node_id: str) -> int:
    return 0 if int(node_id[0], 16) >= 8 else 1


def test_find_nearest_sorted_by_xor_distance():
    table = RoutingTable(make_node_id(0))
    peers = [make_peer(i % 2, 8000 + i) for i in range(10)]
    for p in peers:
        table.try_insert(p)
    target = make_node_id(1)
    nearest = table.find_nearest(target)
    distances = [xor_distance(p.node_id, target) for p in nearest]
    assert distances == sorted(distances)


def test_bucket_splits_when_own_bucket_full():
    own_id = make_node_id(0)
    table = RoutingTable(own_id)
    for i in range(K_BUCKET_SIZE + 5):
        table.try_insert(make_peer(i % 2, 8000 + i))
    assert len(table._buckets) > 1
    for bucket in table._buckets:
        assert len(bucket.peers) <= K_BUCKET_SIZE


def test_full_foreign_bucket_goes_to_replacement_cache():
    own_id = make_node_id(0)
    far_bit = opposite_first_bit(own_id)
    table = RoutingTable(own_id)
    for i in range(K_BUCKET_SIZE + 1):
        table.try_insert(make_peer(far_bit, 8000 + i))
    extra = make_peer(far_bit, 9999)
    status = table.try_insert(extra)
    assert status == "in_replacement"
    bucket = table.get_bucket_for(extra.node_id)
    assert any(p.node_id == extra.node_id for p in bucket.replacement_cache)
    assert len(bucket.peers) <= K_BUCKET_SIZE


def test_replacement_cache_dedup():
    own_id = make_node_id(0)
    far_bit = opposite_first_bit(own_id)
    table = RoutingTable(own_id)
    for i in range(K_BUCKET_SIZE + 1):
        table.try_insert(make_peer(far_bit, 8000 + i))
    extra = make_peer(far_bit, 9999)
    table.try_insert(extra)
    table.try_insert(Peer(extra.node_id, extra.ip, extra.port))
    bucket = table.get_bucket_for(extra.node_id)
    cache_ids = [p.node_id for p in bucket.replacement_cache]
    assert cache_ids.count(extra.node_id) == 1


def test_remove_peer_promotes_from_replacement_cache():
    own_id = make_node_id(0)
    far_bit = opposite_first_bit(own_id)
    table = RoutingTable(own_id)
    for i in range(K_BUCKET_SIZE + 1):
        table.try_insert(make_peer(far_bit, 8000 + i))
    extra = make_peer(far_bit, 9999)
    table.try_insert(extra)
    bucket = table.get_bucket_for(extra.node_id)
    victim = bucket.peers[0]
    table.remove_peer(victim.node_id)
    assert not any(p.node_id == victim.node_id for p in bucket.peers)
    assert len(bucket.peers) <= K_BUCKET_SIZE


async def test_dead_oldest_replaced_without_duplicates():
    # Regression: evicting a dead oldest peer must insert the new peer
    # exactly once and never grow the bucket past K.
    server = KademliaServer()
    far_bit = opposite_first_bit(server.own_peer.node_id)

    async def dead_ping(peer):
        return False

    server.ping = dead_ping

    for i in range(K_BUCKET_SIZE + 1):
        await server.try_add_peer(make_peer(far_bit, 8000 + i))

    newcomer = make_peer(far_bit, 9999)
    oldest_before = server.routing_table.get_bucket_for(newcomer.node_id).oldest()
    await server.try_add_peer(newcomer)

    bucket = server.routing_table.get_bucket_for(newcomer.node_id)
    ids = [p.node_id for p in bucket.peers]
    assert len(ids) == len(set(ids)), "duplicate peer in bucket"
    assert len(bucket.peers) <= K_BUCKET_SIZE
    assert ids.count(newcomer.node_id) == 1
    assert oldest_before.node_id not in ids


async def test_own_node_id_never_inserted():
    server = KademliaServer()
    await server.try_add_peer(Peer(server.own_peer.node_id, "127.0.0.1", 8000))
    assert server.routing_table.all_peers() == []

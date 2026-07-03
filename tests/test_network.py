import asyncio

import pytest

from kademlia import KademliaServer


async def spawn_network(count: int, serialization: str = "json"):
    servers = []
    for _ in range(count):
        server = KademliaServer(serialization=serialization)
        await server.listen(port=0)
        servers.append(server)
    first = servers[0]
    for server in servers[1:]:
        await server.bootstrap([(first.own_peer.ip, first.own_peer.port)])
    return servers


def stop_all(servers):
    for server in servers:
        server.stop()


@pytest.mark.parametrize("serialization", ["json", "bencode"])
async def test_set_get_str(serialization):
    servers = await spawn_network(5, serialization)
    try:
        await servers[1].set("greeting", "hello world")
        for server in servers:
            assert await server.get("greeting") == "hello world"
    finally:
        stop_all(servers)


@pytest.mark.parametrize("serialization", ["json", "bencode"])
async def test_set_get_bytes(serialization):
    servers = await spawn_network(5, serialization)
    try:
        payload = b"\x00\x01\xffbinary blob"
        await servers[2].set("blob", payload)
        value = await servers[4].get("blob")
        assert value == payload
        assert isinstance(value, bytes)
    finally:
        stop_all(servers)


async def test_get_missing_key_returns_none():
    servers = await spawn_network(3)
    try:
        assert await servers[1].get("nope") is None
    finally:
        stop_all(servers)


async def test_value_survives_publisher_departure():
    servers = await spawn_network(5)
    try:
        await servers[1].set("durable", "still here")
        servers[1].stop()
        assert await servers[3].get("durable") == "still here"
    finally:
        stop_all(servers)


async def test_bootstrap_populates_routing_tables():
    servers = await spawn_network(4)
    try:
        for server in servers:
            assert len(server.routing_table.all_peers()) >= 1
    finally:
        stop_all(servers)


async def test_response_from_wrong_source_dropped():
    server = KademliaServer(verify_response_source=True)
    future = asyncio.get_running_loop().create_future()
    server.pending_queries["qid"] = (future, ("127.0.0.1", 8000))

    server.deliver_response("qid", {"type": "pong"}, addr=("127.0.0.1", 9999))
    assert not future.done()

    server.deliver_response("qid", {"type": "pong"}, addr=("127.0.0.1", 8000))
    assert future.done()
    assert future.result() == {"type": "pong"}


async def test_response_source_check_can_be_disabled():
    server = KademliaServer(verify_response_source=False)
    future = asyncio.get_running_loop().create_future()
    server.pending_queries["qid"] = (future, ("127.0.0.1", 8000))

    server.deliver_response("qid", {"type": "pong"}, addr=("127.0.0.1", 9999))
    assert future.done()

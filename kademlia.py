import asyncio
import hashlib
import logging
import secrets
import socket
import time
from typing import Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

NODE_ID_BITS = 160
NODE_ID_BYTES = NODE_ID_BITS // 8
K_BUCKET_SIZE = 20
ALPHA_CONCURRENCY = 3
QUERY_TIMEOUT_SECONDS = 2.0
BUCKET_REFRESH_INTERVAL = 3600
NON_PUBLISHER_RESTORE_INTERVAL = 3600
ORIGINAL_PUBLISHER_REPUBLISH_INTERVAL = 86400
KEY_EXPIRY_SECONDS = 86410
QUERY_ID_HEX_LENGTH = 16
MIN_CACHE_TTL_SECONDS = 600
DEFAULT_BIND_IP = "127.0.0.1"
_MAX_DISPATCH_TASKS = 64
_B64_MARKER_KEY = "__bytes_b64__"
_BYTES_MARKER = b"\x00bytes\x00"

Value = Union[str, bytes]


def xor_distance(hex_id_a: str, hex_id_b: str) -> int:
    return int(hex_id_a, 16) ^ int(hex_id_b, 16)


def generate_node_id() -> str:
    random_bytes = secrets.token_bytes(NODE_ID_BYTES)
    return hashlib.sha1(random_bytes).hexdigest()


def hash_key_to_node_id(key: str) -> str:
    digest = hashlib.sha1(key.encode()).digest()
    return digest.hex()


def generate_query_id() -> str:
    return secrets.token_hex(QUERY_ID_HEX_LENGTH // 2)


def node_id_to_binary(node_id: str) -> str:
    return bin(int(node_id, 16))[2:].zfill(NODE_ID_BITS)


def compute_cache_ttl(own_node_id: str, target_id: str) -> float:
    distance = xor_distance(own_node_id, target_id)
    ttl = KEY_EXPIRY_SECONDS // (distance.bit_length() + 1)
    return max(MIN_CACHE_TTL_SECONDS, ttl)


class Peer:
    def __init__(self, node_id: str, ip: str, port: int):
        self.node_id = node_id
        self.ip = ip
        self.port = port
        self.last_seen = time.time()

    def mark_seen(self):
        self.last_seen = time.time()

    def to_dict(self) -> dict:
        return {"node_id": self.node_id, "ip": self.ip, "port": self.port}

    @classmethod
    def from_dict(cls, data: dict) -> "Peer":
        return cls(data["node_id"], data["ip"], data["port"])

    def __str__(self) -> str:
        return f"{self.node_id[:8]}@{self.ip}:{self.port}"


class KBucket:
    def __init__(self, prefix: str = ""):
        self.prefix = prefix
        self.peers: List[Peer] = []
        self.replacement_cache: List[Peer] = []

    def contains(self, node_id: str) -> bool:
        return any(p.node_id == node_id for p in self.peers)

    def update_existing(self, node_id: str) -> bool:
        for i, peer in enumerate(self.peers):
            if peer.node_id == node_id:
                peer.mark_seen()
                self.peers.append(self.peers.pop(i))
                return True
        for i, peer in enumerate(self.replacement_cache):
            if peer.node_id == node_id:
                peer.mark_seen()
                self.replacement_cache.append(self.replacement_cache.pop(i))
                return True
        return False

    def has_capacity(self) -> bool:
        return len(self.peers) < K_BUCKET_SIZE

    def add_new(self, peer: Peer):
        self.peers.append(peer)

    def add_to_replacement_cache(self, peer: Peer):
        if not any(p.node_id == peer.node_id for p in self.replacement_cache):
            self.replacement_cache.append(peer)

    def evict_oldest_and_promote(self):
        if self.peers:
            self.peers.pop(0)
        if self.replacement_cache:
            self.peers.append(self.replacement_cache.pop(0))

    def oldest(self) -> Optional[Peer]:
        return self.peers[0] if self.peers else None

    def get_peers(self) -> List[Peer]:
        return list(self.peers)


class RoutingTable:
    def __init__(self, own_node_id: str):
        self.own_node_id = own_node_id
        self._buckets: List[KBucket] = [KBucket(prefix="")]

    def _bucket_for(self, node_id: str) -> KBucket:
        binary = node_id_to_binary(node_id)
        for bucket in self._buckets:
            if binary.startswith(bucket.prefix):
                return bucket
        return self._buckets[-1]

    def _owns_bucket(self, bucket: KBucket) -> bool:
        own_binary = node_id_to_binary(self.own_node_id)
        return own_binary.startswith(bucket.prefix)

    def _split_bucket(self, bucket: KBucket):
        prefix_zero = KBucket(prefix=bucket.prefix + "0")
        prefix_one = KBucket(prefix=bucket.prefix + "1")
        for peer in bucket.peers:
            binary = node_id_to_binary(peer.node_id)
            if binary.startswith(prefix_zero.prefix):
                prefix_zero.peers.append(peer)
            else:
                prefix_one.peers.append(peer)
        for peer in bucket.replacement_cache:
            binary = node_id_to_binary(peer.node_id)
            if binary.startswith(prefix_zero.prefix):
                prefix_zero.replacement_cache.append(peer)
            else:
                prefix_one.replacement_cache.append(peer)
        index = self._buckets.index(bucket)
        self._buckets[index:index + 1] = [prefix_zero, prefix_one]

    def try_insert(self, peer: Peer) -> str:
        while True:
            bucket = self._bucket_for(peer.node_id)
            if bucket.update_existing(peer.node_id):
                return "added"
            if bucket.has_capacity():
                bucket.add_new(peer)
                return "added"
            if self._owns_bucket(bucket):
                self._split_bucket(bucket)
                continue
            bucket.add_to_replacement_cache(peer)
            return "in_replacement"

    def remove_peer(self, node_id: str):
        bucket = self._bucket_for(node_id)
        bucket.peers = [p for p in bucket.peers if p.node_id != node_id]
        if bucket.replacement_cache:
            bucket.peers.append(bucket.replacement_cache.pop(0))

    def find_nearest(self, target_id: str, count: int = K_BUCKET_SIZE) -> List[Peer]:
        all_peers = [p for bucket in self._buckets for p in bucket.get_peers()]
        all_peers.sort(key=lambda p: xor_distance(p.node_id, target_id))
        return all_peers[:count]

    def get_bucket_for(self, node_id: str) -> KBucket:
        return self._bucket_for(node_id)

    def all_peers(self) -> List[Peer]:
        return [p for bucket in self._buckets for p in bucket.get_peers()]

    def stale_buckets(self) -> List[KBucket]:
        cutoff = time.time() - BUCKET_REFRESH_INTERVAL
        return [b for b in self._buckets if b.peers and b.peers[-1].last_seen < cutoff]


class KademliaProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: "KademliaServer"):
        self.server = server
        self.transport = None
        self._active_dispatch_tasks: int = 0

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if self._active_dispatch_tasks >= _MAX_DISPATCH_TASKS:
            logger.warning(f"[kad] dispatch queue full ({_MAX_DISPATCH_TASKS}), dropping packet from {addr}")
            return
        self._active_dispatch_tasks += 1
        task = asyncio.create_task(self._dispatch(data, addr))
        task.add_done_callback(lambda _: setattr(self, '_active_dispatch_tasks', self._active_dispatch_tasks - 1))

    async def _dispatch(self, data, addr):
        try:
            msg = self.server.decode(data)
            msg_type = msg.get("type")
            handler = getattr(self, f"_handle_{msg_type}", None)
            if handler:
                await handler(msg, addr)
        except Exception as e:
            logger.error(f"Error handling message from {addr}: {e}")

    def _send_to(self, payload: dict, addr):
        self.transport.sendto(self.server.encode(payload), addr)

    def _register_sender(self, msg: dict, addr):
        sender = Peer.from_dict(msg["sender"])
        sender.ip = addr[0]
        asyncio.create_task(self.server.try_add_peer(sender))

    async def _handle_ping(self, msg, addr):
        self._register_sender(msg, addr)
        self._send_to({"type": "pong", "sender": self.server.own_peer.to_dict(), "id": msg["id"]}, addr)

    async def _handle_pong(self, msg, addr):
        self._register_sender(msg, addr)
        self.server.deliver_response(msg["id"], msg)

    async def _handle_find_node(self, msg, addr):
        self._register_sender(msg, addr)
        nearest = self.server.routing_table.find_nearest(msg["target"])
        self._send_to({
            "type": "find_node_res",
            "sender": self.server.own_peer.to_dict(),
            "id": msg["id"],
            "peers": [p.to_dict() for p in nearest],
        }, addr)

    async def _handle_find_node_res(self, msg, addr):
        self.server.deliver_response(msg["id"], msg)

    async def _handle_set(self, msg, addr):
        self._register_sender(msg, addr)
        self.server._store_remote_value(msg["key"], msg["value"])
        self._send_to({"type": "set_res", "sender": self.server.own_peer.to_dict(), "id": msg["id"]}, addr)

    async def _handle_set_res(self, msg, addr):
        self.server.deliver_response(msg["id"], msg)

    async def _handle_set_cached(self, msg, addr):
        self._register_sender(msg, addr)
        self.server._store_cached_value(msg["key"], msg["value"], msg["target_id"])
        self._send_to({"type": "set_cached_res", "sender": self.server.own_peer.to_dict(), "id": msg["id"]}, addr)

    async def _handle_set_cached_res(self, msg, addr):
        self.server.deliver_response(msg["id"], msg)

    async def _handle_check_store(self, msg, addr):
        self._register_sender(msg, addr)
        key = msg["key"]
        entry = self.server.data_store.get(key)
        has_key = entry is not None and not entry.is_expired()
        self._send_to({
            "type": "check_store_res",
            "sender": self.server.own_peer.to_dict(),
            "id": msg["id"],
            "has_key": has_key,
        }, addr)

    async def _handle_check_store_res(self, msg, addr):
        self.server.deliver_response(msg["id"], msg)

    async def _handle_get(self, msg, addr):
        self._register_sender(msg, addr)
        entry = self.server.data_store.get(msg["key"])
        self._send_to({
            "type": "get_res",
            "sender": self.server.own_peer.to_dict(),
            "id": msg["id"],
            "value": entry.value if entry and not entry.is_expired() else None,
        }, addr)

    async def _handle_get_res(self, msg, addr):
        self.server.deliver_response(msg["id"], msg)

    async def _handle_find_value(self, msg, addr):
        self._register_sender(msg, addr)
        key = msg["key"]
        entry = self.server.data_store.get(key)
        if entry and not entry.is_expired():
            self._send_to({
                "type": "find_value_res",
                "sender": self.server.own_peer.to_dict(),
                "id": msg["id"],
                "value": entry.value,
            }, addr)
        else:
            nearest = self.server.routing_table.find_nearest(hash_key_to_node_id(key))
            self._send_to({
                "type": "find_value_res",
                "sender": self.server.own_peer.to_dict(),
                "id": msg["id"],
                "peers": [p.to_dict() for p in nearest],
            }, addr)

    async def _handle_find_value_res(self, msg, addr):
        self.server.deliver_response(msg["id"], msg)


def json_encode(payload: dict) -> bytes:
    import json
    return json.dumps(_strip_none(_tag_bytes_for_json(payload))).encode()


def json_decode(data: bytes) -> dict:
    import json
    return _untag_bytes_from_json(json.loads(data.decode()))


def _tag_bytes_for_json(value):
    import base64
    if isinstance(value, bytes):
        return {_B64_MARKER_KEY: base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {k: _tag_bytes_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_tag_bytes_for_json(v) for v in value]
    return value


def _untag_bytes_from_json(value):
    import base64
    if isinstance(value, dict):
        if set(value.keys()) == {_B64_MARKER_KEY}:
            return base64.b64decode(value[_B64_MARKER_KEY])
        return {k: _untag_bytes_from_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_untag_bytes_from_json(v) for v in value]
    return value


def _strip_none(value):
    if isinstance(value, dict):
        return {k: _strip_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_strip_none(v) for v in value]
    return value


def bencode_encode(payload: dict) -> bytes:
    return _bencode_value(_strip_none(payload))


def _bencode_value(value) -> bytes:
    if isinstance(value, bool):
        return b"i" + (b"1" if value else b"0") + b"e"
    if isinstance(value, int):
        return b"i" + str(value).encode() + b"e"
    if isinstance(value, str):
        raw = value.encode()
        return str(len(raw)).encode() + b":" + raw
    if isinstance(value, bytes):
        tagged = _BYTES_MARKER + value
        return str(len(tagged)).encode() + b":" + tagged
    if isinstance(value, list):
        return b"l" + b"".join(_bencode_value(v) for v in value) + b"e"
    if isinstance(value, dict):
        items = sorted(value.items(), key=lambda kv: kv[0])
        body = b"".join(_bencode_value(k) + _bencode_value(v) for k, v in items)
        return b"d" + body + b"e"
    raise TypeError(f"bencode cannot encode {type(value)}")


def bencode_decode(data: bytes) -> dict:
    value, offset = _bencode_parse(data, 0)
    if offset != len(data):
        raise ValueError("trailing data after bencoded value")
    return value


def _bencode_parse(data: bytes, offset: int):
    marker = data[offset:offset + 1]
    if marker == b"i":
        end = data.index(b"e", offset)
        return int(data[offset + 1:end]), end + 1
    if marker == b"l":
        offset += 1
        result = []
        while data[offset:offset + 1] != b"e":
            item, offset = _bencode_parse(data, offset)
            result.append(item)
        return result, offset + 1
    if marker == b"d":
        offset += 1
        result = {}
        while data[offset:offset + 1] != b"e":
            key, offset = _bencode_parse(data, offset)
            val, offset = _bencode_parse(data, offset)
            result[key.decode() if isinstance(key, bytes) else key] = val
        return result, offset + 1
    if marker.isdigit():
        colon = data.index(b":", offset)
        length = int(data[offset:colon])
        start = colon + 1
        raw = data[start:start + length]
        if raw.startswith(_BYTES_MARKER):
            return raw[len(_BYTES_MARKER):], start + length
        try:
            return raw.decode(), start + length
        except UnicodeDecodeError:
            return raw, start + length
    raise ValueError(f"invalid bencode at offset {offset}")


class StoredValue:
    def __init__(self, value: Value, is_original_publisher: bool, expires_at: float = 0.0):
        self.value = value
        self.stored_at = time.time()
        self.last_republished_at = time.time()
        self.is_original_publisher = is_original_publisher
        self.expires_at = expires_at if expires_at > 0.0 else time.time() + KEY_EXPIRY_SECONDS

    def is_expired(self) -> bool:
        return time.time() > self.expires_at

    def needs_republish(self) -> bool:
        interval = ORIGINAL_PUBLISHER_REPUBLISH_INTERVAL if self.is_original_publisher else NON_PUBLISHER_RESTORE_INTERVAL
        return time.time() - self.last_republished_at >= interval

    def mark_republished(self):
        self.last_republished_at = time.time()


SERIALIZATION_CODECS = {
    "json": (json_encode, json_decode),
    "bencode": (bencode_encode, bencode_decode),
}


class KademliaServer:
    def __init__(self, serialization: str = "json"):
        if serialization not in SERIALIZATION_CODECS:
            raise ValueError(f"unknown serialization: {serialization!r}, expected one of {list(SERIALIZATION_CODECS)}")
        self.serialization = serialization
        self.encode, self.decode = SERIALIZATION_CODECS[serialization]

        self.data_store: Dict[str, StoredValue] = {}
        self.pending_queries: Dict[str, asyncio.Future] = {}

        node_id = generate_node_id()
        self.own_peer = Peer(node_id, "0.0.0.0", 0)
        self.routing_table = RoutingTable(node_id)
        self.protocol: Optional[KademliaProtocol] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._republish_task: Optional[asyncio.Task] = None
        self._expiry_task: Optional[asyncio.Task] = None

    async def listen(self, port: int, ip: str = DEFAULT_BIND_IP):
        loop = asyncio.get_running_loop()
        self.own_peer.ip = ip
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((ip, port))
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: KademliaProtocol(self),
            sock=sock
        )
        self.own_peer.port = transport.get_extra_info("sockname")[1]
        self.protocol = protocol
        self._refresh_task = asyncio.create_task(self._periodic_bucket_refresh())
        self._republish_task = asyncio.create_task(self._periodic_republish())
        self._expiry_task = asyncio.create_task(self._periodic_expiry())
        logger.info(f"Kademlia node listening on {ip}:{self.own_peer.port} with ID {self.own_peer.node_id[:8]}")

    async def bootstrap(self, nodes: List[Tuple[str, int]]):
        for ip, port in nodes:
            query_id = generate_query_id()
            query = {"type": "ping", "id": query_id, "sender": self.own_peer.to_dict()}
            future = asyncio.get_running_loop().create_future()
            self.pending_queries[query_id] = future
            try:
                self.protocol.transport.sendto(self.encode(query), (ip, port))
                res = await asyncio.wait_for(future, timeout=QUERY_TIMEOUT_SECONDS)
                if res:
                    real_peer = Peer.from_dict(res["sender"])
                    real_peer.ip = ip
                    await self.try_add_peer(real_peer)
                    await self.find_node(self.own_peer.node_id)
                    await self._refresh_distant_buckets()
            except asyncio.TimeoutError:
                logger.debug(f"[bootstrap] timeout connecting to {ip}:{port}")
            finally:
                self.pending_queries.pop(query_id, None)

    async def _refresh_distant_buckets(self):
        refresh_tasks = []
        for bucket in self.routing_table._buckets:
            random_id_in_bucket = self._random_id_for_prefix(bucket.prefix)
            refresh_tasks.append(self.find_node(random_id_in_bucket))
        if refresh_tasks:
            await asyncio.gather(*refresh_tasks)

    def _random_id_for_prefix(self, prefix: str) -> str:
        suffix_len = NODE_ID_BITS - len(prefix)
        random_suffix = bin(secrets.randbits(suffix_len))[2:].zfill(suffix_len)
        binary = prefix + random_suffix
        return hex(int(binary, 2))[2:].zfill(NODE_ID_BYTES * 2)

    async def ping(self, peer: Peer) -> bool:
        return await self._send_query(peer, {"type": "ping"}) is not None

    async def try_add_peer(self, peer: Peer):
        if peer.node_id == self.own_peer.node_id:
            return
        status = self.routing_table.try_insert(peer)
        if status == "in_replacement":
            bucket = self.routing_table.get_bucket_for(peer.node_id)
            oldest = bucket.oldest()
            if not oldest:
                return
            alive = await self.ping(oldest)
            if not alive and bucket.oldest() is oldest:
                bucket.evict_oldest_and_promote()
                bucket.add_new(peer)

    async def find_node(self, target_id: str) -> List[Peer]:
        closest = self.routing_table.find_nearest(target_id, K_BUCKET_SIZE)
        if not closest:
            return []

        seen_ids = {self.own_peer.node_id} | {p.node_id for p in closest}
        queried_ids: set = set()
        in_flight: Dict[asyncio.Task, Peer] = {}

        async def process_response(peer: Peer, res: Optional[dict]):
            if res and "peers" in res:
                for peer_dict in res["peers"]:
                    candidate = Peer.from_dict(peer_dict)
                    if candidate.node_id not in seen_ids:
                        seen_ids.add(candidate.node_id)
                        await self.try_add_peer(candidate)

        while True:
            closest = self.routing_table.find_nearest(target_id, K_BUCKET_SIZE)
            unqueried = [p for p in closest if p.node_id not in queried_ids]

            while len(in_flight) < ALPHA_CONCURRENCY and unqueried:
                peer = unqueried.pop(0)
                queried_ids.add(peer.node_id)
                task = asyncio.create_task(self._send_query(peer, {"type": "find_node", "target": target_id}))
                in_flight[task] = peer

            if not in_flight:
                break

            done, _ = await asyncio.wait(in_flight.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                peer = in_flight.pop(task)
                await process_response(peer, task.result())

            closest = self.routing_table.find_nearest(target_id, K_BUCKET_SIZE)
            all_queried = {p.node_id for p in closest}.issubset(queried_ids | {self.own_peer.node_id})
            nothing_in_flight = not in_flight
            if all_queried and nothing_in_flight:
                break

        for task in in_flight:
            task.cancel()
        return self.routing_table.find_nearest(target_id, K_BUCKET_SIZE)

    async def find_value(self, key: str) -> Tuple[Optional[Value], List[Peer]]:
        target_id = hash_key_to_node_id(key)
        closest = self.routing_table.find_nearest(target_id, K_BUCKET_SIZE)
        if not closest:
            return None, []

        seen_ids = {self.own_peer.node_id} | {p.node_id for p in closest}
        queried_ids: set = set()
        queried_peers: Dict[str, Peer] = {}
        in_flight: Dict[asyncio.Task, Peer] = {}

        while True:
            closest = self.routing_table.find_nearest(target_id, K_BUCKET_SIZE)
            unqueried = [p for p in closest if p.node_id not in queried_ids]

            while len(in_flight) < ALPHA_CONCURRENCY and unqueried:
                peer = unqueried.pop(0)
                queried_ids.add(peer.node_id)
                queried_peers[peer.node_id] = peer
                task = asyncio.create_task(self._send_query(peer, {"type": "find_value", "key": key}))
                in_flight[task] = peer

            if not in_flight:
                break

            done, _ = await asyncio.wait(in_flight.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                peer = in_flight.pop(task)
                res = task.result()
                if not res:
                    continue
                if "value" in res:
                    for t in in_flight:
                        t.cancel()
                    non_holders = [p for p in queried_peers.values() if p.node_id != peer.node_id]
                    await self._cache_value_on_path(key, res["value"], non_holders)
                    return res["value"], closest

                if "peers" in res:
                    for peer_dict in res["peers"]:
                        candidate = Peer.from_dict(peer_dict)
                        if candidate.node_id not in seen_ids:
                            seen_ids.add(candidate.node_id)
                            await self.try_add_peer(candidate)

            closest = self.routing_table.find_nearest(target_id, K_BUCKET_SIZE)
            all_queried = {p.node_id for p in closest}.issubset(queried_ids | {self.own_peer.node_id})
            if all_queried and not in_flight:
                break

        return None, closest

    async def _cache_value_on_path(self, key: str, value: Value, candidates: List[Peer]):
        target_id = hash_key_to_node_id(key)
        candidates_sorted = sorted(candidates, key=lambda p: xor_distance(p.node_id, target_id))
        if candidates_sorted:
            closest_non_holder = candidates_sorted[0]
            await self._send_query(closest_non_holder, {
                "type": "set_cached",
                "key": key,
                "value": value,
                "target_id": target_id,
            })

    async def set(self, key: str, value: Value):
        target_id = hash_key_to_node_id(key)
        closest = await self.find_node(target_id)
        self.data_store[key] = StoredValue(value, is_original_publisher=True)
        check_tasks = [
            self._send_query(p, {"type": "check_store", "key": key, "size": len(value)})
            for p in closest
        ]
        check_results = await asyncio.gather(*check_tasks)
        store_tasks = [
            self._send_query(peer, {"type": "set", "key": key, "value": value})
            for peer, res in zip(closest, check_results)
            if res is None or not res.get("has_key", False)
        ]
        await asyncio.gather(*store_tasks)

    def _store_remote_value(self, key: str, value: Value):
        self.data_store[key] = StoredValue(value, is_original_publisher=False)

    def _store_cached_value(self, key: str, value: Value, target_id: str):
        ttl = compute_cache_ttl(self.own_peer.node_id, target_id)
        expires_at = time.time() + ttl
        self.data_store[key] = StoredValue(value, is_original_publisher=False, expires_at=expires_at)

    async def get(self, key: str) -> Optional[Value]:
        entry = self.data_store.get(key)
        if entry and not entry.is_expired():
            return entry.value
        value, _ = await self.find_value(key)
        return value

    async def _send_query(self, peer: Peer, query: dict) -> Optional[dict]:
        query_id = generate_query_id()
        query["id"] = query_id
        query["sender"] = self.own_peer.to_dict()
        future = asyncio.get_running_loop().create_future()
        self.pending_queries[query_id] = future
        try:
            self.protocol.transport.sendto(self.encode(query), (peer.ip, peer.port))
            return await asyncio.wait_for(future, timeout=QUERY_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            self.routing_table.remove_peer(peer.node_id)
            return None
        finally:
            self.pending_queries.pop(query_id, None)

    async def _periodic_bucket_refresh(self):
        while True:
            await asyncio.sleep(BUCKET_REFRESH_INTERVAL)
            for bucket in self.routing_table.stale_buckets():
                random_id = self._random_id_for_prefix(bucket.prefix)
                await self.find_node(random_id)

    async def _periodic_republish(self):
        while True:
            await asyncio.sleep(NON_PUBLISHER_RESTORE_INTERVAL)
            for key, entry in list(self.data_store.items()):
                if entry.is_expired() or not entry.needs_republish():
                    continue
                if entry.is_original_publisher:
                    await self.set(key, entry.value)
                else:
                    closest = await self.find_node(hash_key_to_node_id(key))
                    store_tasks = [self._send_query(p, {"type": "set", "key": key, "value": entry.value}) for p in closest]
                    await asyncio.gather(*store_tasks)
                entry.mark_republished()

    async def _periodic_expiry(self):
        while True:
            await asyncio.sleep(BUCKET_REFRESH_INTERVAL)
            self.data_store = {k: v for k, v in self.data_store.items() if not v.is_expired()}

    def deliver_response(self, query_id: str, msg: dict):
        future = self.pending_queries.get(query_id)
        if future and not future.done():
            future.set_result(msg)

    def stop(self):
        for task in (self._refresh_task, self._republish_task, self._expiry_task):
            if task:
                task.cancel()
        if self.protocol and self.protocol.transport:
            try:
                self.protocol.transport.close()
            except RuntimeError:
                pass

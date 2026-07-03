# kademlia-dynamic

Lightweight, async-first Kademlia DHT implementation in pure Python with zero external dependencies. Distinct from the [`kademlia`](https://pypi.org/project/kademlia/) package on PyPI, imports as `kademlia_dynamic`.

## Installation

```bash
pip install kademlia-dynamic
```

Or from source:

```bash
cd kademlia_dynamic
python -m pip install -e .
```

```python
from kademlia_dynamic import KademliaServer, Peer, generate_node_id
```

## Quick Start

```python
import asyncio
from kademlia_dynamic import KademliaServer

async def main():
    server = KademliaServer()  # or KademliaServer(serialization="bencode")
    await server.listen(port=8000)  # defaults to 127.0.0.1; pass ip="0.0.0.0" to accept LAN/WAN peers

    await server.bootstrap([("192.168.1.100", 8000)])

    await server.set("my_key", "my_value")       # str
    await server.set("my_blob", b"\x00binary")   # or bytes
    value = await server.get("my_key")
    print(value)

    server.stop()

asyncio.run(main())
```

## API Reference

### KademliaServer

**Main DHT node class**

```python
KademliaServer(serialization: str = "json", verify_response_source: bool = True)
```

- `serialization` — `"json"` (default) or `"bencode"` wire format
- `verify_response_source` — when `True` (default), responses are only accepted from the IP:port the query was sent to; forged responses from other sources are dropped. Set to `False` if peers legitimately reply from a different address (e.g. some NAT setups).

Additional keyword-only tuning arguments (`k`, `alpha`, `query_timeout`, ...) are listed under [Configuration](#configuration).

#### Methods

- `async listen(port: int, ip: str = "127.0.0.1")` — Start listening for UDP packets. Pass `ip="0.0.0.0"` to accept connections from other machines (LAN/WAN). Only do this behind a firewall/NAT you control.
- `async bootstrap(nodes: List[Tuple[str, int]])` — Join network from bootstrap nodes
- `async set(key: str, value: str | bytes)` — Store value in DHT
- `async get(key: str) -> Optional[str | bytes]` — Retrieve value from DHT, same type as stored
- `async find_node(target_id: str) -> List[Peer]` — Find peers close to target ID
- `async find_value(key: str) -> Tuple[Optional[str | bytes], List[Peer]]` — Search for value, return closest peers if not found
- `stop()` — Shut down node and close transport

### Peer

**Represents a network node**

```python
peer = Peer(node_id="abc123...", ip="192.168.1.100", port=8000)
peer.to_dict()  # Serializable dict
Peer.from_dict(data)  # Deserialize
```

### Utility Functions

- `generate_node_id() -> str` — Generate random 160-bit node ID (SHA1)
- `hash_key_to_node_id(key: str) -> str` — Hash key to node ID space
- `xor_distance(hex_id_a: str, hex_id_b: str) -> int` — Compute XOR distance

## Comparison to Canonical Kademlia

Canonical references: the Kademlia paper (Maymounkov & Mazières, 2002) and BEP 5 (the BitTorrent DHT protocol).

### Similarities

- 160-bit node IDs (SHA1)
- XOR distance metric
- K-buckets (K=20) with replacement cache
- Alpha concurrency (α=3)
- Ping, find_node, store/retrieve operations
- Periodic bucket refresh, value republishing, expiry cleanup
- Asynchronous UDP protocol

### Differences vs canonical Kademlia / BEP 5

| Feature                         | This Implementation                | Canonical Kademlia / BEP 5          |
| ------------------------------- | ---------------------------------- | ----------------------------------- |
| **Language**                    | Python                             | (Language-agnostic spec)            |
| **Async Model**                 | asyncio                            | Not specified (implementation-dependent) |
| **Serialization**               | JSON (default) or Bencode          | Bencode (bencoding)                 |
| **Value TTL**                   | Dynamic (distance-based)           | Paper: distance-based; BEP 5: not specified |
| **Original Publisher Tracking** | Yes                                | Paper: 24h republish rule; BEP 5: no |
| **Cached Value TTL**            | Inversely proportional to distance | Paper: same idea (§2.5); BEP 5: fixed short TTL |
| **RPC Protocol**                | JSON or bencoded dict over UDP     | Bencoded dict over UDP              |
| **Socket Binding**              | User-specified IP                  | Auto-detect                         |

### Behavioral Notes

- **Node Discovery:** Includes implicit peer discovery via sender fields in all responses (not explicit in BEP 5)
- **Cache TTL:** Cached values expire faster (shorter TTL) for distant nodes, reducing stale caches
- **Bucket Refresh:** Proactive refresh every 3600s (1h) of buckets that haven't seen activity
- **Value Expiry:** Original publishers republish every 24h; non-publishers every 1h

## Configuration

All tunables are per-server constructor arguments (keyword-only). The module-level constants in `kademlia_dynamic/kademlia.py` are only their defaults.

```python
server = KademliaServer(
    serialization="json",                          # or "bencode"
    verify_response_source=True,                   # drop responses from unexpected addresses
    k=20,                                          # peers per bucket (K_BUCKET_SIZE)
    alpha=3,                                       # parallel queries (ALPHA_CONCURRENCY)
    query_timeout=2.0,                             # RPC timeout, seconds
    bucket_refresh_interval=3600,                  # refresh stale buckets + expiry sweep (s)
    key_expiry_seconds=86410,                      # value TTL (24h + 10s)
    non_publisher_restore_interval=3600,           # cache restore interval (1h)
    original_publisher_republish_interval=86400,   # publisher republish (24h)
    min_cache_ttl_seconds=600,                     # minimum cached value TTL (10m)
    max_dispatch_tasks=64,                         # backpressure: max concurrent inbound handlers
)
```

The bind address is chosen per node via `listen(port, ip=...)`. Node ID width (160-bit) is fixed — it is tied to SHA1.

All nodes in a network should use the same `k` and `alpha` for predictable lookup behavior, and must use the same serialization.

## Design Notes

### JSON vs Bencode

Both are built in, pure Python, zero dependencies. Pick per-server via the `serialization` constructor arg:

```python
KademliaServer(serialization="json")      # default: human-readable, easy to debug
KademliaServer(serialization="bencode")   # BitTorrent-style bencode wire format
```

All peers in a network must use the same serialization to interoperate. `None` values are omitted from encoded messages in both formats (bencode has no null type); readers treat a missing key as `None`.

One bencode edge case: a `str` value that begins with the internal byte marker `\x00bytes\x00` will round-trip back as `bytes`. Avoid leading NUL bytes in string values (or use the JSON codec, which does not have this ambiguity).

### Values: str or bytes

`set()`/`get()` accept and return either `str` or `bytes`. On the wire, bencode stores bytes natively; JSON (which has no binary type) base64-encodes bytes transparently and decodes them back on receipt. Type is preserved round-trip — a `bytes` value in never comes back as `str`, and vice versa.

### Network Exposure

`listen()` binds `127.0.0.1` by default. To join a real network, pass `ip="0.0.0.0"` (binds all interfaces) and ensure the UDP port is open/forwarded on your firewall/router.

### Thread Safety

Not thread-safe. Designed for single-threaded asyncio use. For multi-threaded access, wrap in locks or run each node in its own event loop.

### Backpressure

Datagram dispatch queue limits concurrent inbound handler tasks (`max_dispatch_tasks`, default 64). Excess packets are dropped with a warning log.

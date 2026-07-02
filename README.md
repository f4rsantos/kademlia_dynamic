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
KademliaServer(serialization: str = "json")  # or "bencode"
```

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

## Comparison to Canonical Kademlia (BEP 20)

### Similarities

- 160-bit node IDs (SHA1)
- XOR distance metric
- K-buckets (K=20) with replacement cache
- Alpha concurrency (α=3)
- Ping, find_node, store/retrieve operations
- Periodic bucket refresh, value republishing, expiry cleanup
- Asynchronous UDP protocol

### Differences vs BEP 20 reference

| Feature                         | This Implementation                | BEP 20 Kademlia                     |
| ------------------------------- | ---------------------------------- | ----------------------------------- |
| **Language**                    | Python                             | (Language-agnostic spec)            |
| **Async Model**                 | asyncio                            | Blocking (implementation-dependent) |
| **Serialization**               | JSON (default) or Bencode          | Bencode (bencoding)                 |
| **Value TTL**                   | Dynamic (distance-based)           | Fixed intervals                     |
| **Original Publisher Tracking** | Yes                                | Not specified                       |
| **Cached Value TTL**            | Inversely proportional to distance | Fixed short TTL                     |
| **RPC Protocol**                | JSON over UDP                      | Bencoded dict over UDP              |
| **Socket Binding**              | User-specified IP                  | Auto-detect                         |

### Behavioral Notes

- **Node Discovery:** Includes implicit peer discovery via sender fields in all responses (not explicit in BEP 20)
- **Cache TTL:** Cached values expire faster (shorter TTL) for distant nodes, reducing stale caches
- **Bucket Refresh:** Proactive refresh every 3600s (1h) of buckets that haven't seen activity
- **Value Expiry:** Original publishers republish every 24h; non-publishers every 1h

## Configuration

Edit module-level constants in `kademlia_dynamic/kademlia.py`:

```python
K_BUCKET_SIZE = 20                              # Peers per bucket
ALPHA_CONCURRENCY = 3                           # Parallel queries
QUERY_TIMEOUT_SECONDS = 2.0                     # RPC timeout
BUCKET_REFRESH_INTERVAL = 3600                  # Refresh stale buckets (s)
KEY_EXPIRY_SECONDS = 86410                      # Value TTL (24h + 10s)
NON_PUBLISHER_RESTORE_INTERVAL = 3600           # Cache restore interval (1h)
ORIGINAL_PUBLISHER_REPUBLISH_INTERVAL = 86400   # Publisher republish (24h)
MIN_CACHE_TTL_SECONDS = 600                     # Minimum cached value TTL (10m)
```

## Design Notes

### JSON vs Bencode

Both are built in, pure Python, zero dependencies. Pick per-server via the `serialization` constructor arg:

```python
KademliaServer(serialization="json")      # default: human-readable, easy to debug
KademliaServer(serialization="bencode")   # BEP 20-compatible wire format
```

All peers in a network must use the same serialization to interoperate. `None` values are omitted from encoded messages in both formats (bencode has no null type); readers treat a missing key as `None`.

### Values: str or bytes

`set()`/`get()` accept and return either `str` or `bytes`. On the wire, bencode stores bytes natively; JSON (which has no binary type) base64-encodes bytes transparently and decodes them back on receipt. Type is preserved round-trip — a `bytes` value in never comes back as `str`, and vice versa.

### Network Exposure

`listen()` binds `127.0.0.1` by default. To join a real network, pass `ip="0.0.0.0"` (binds all interfaces) and ensure the UDP port is open/forwarded on your firewall/router.

### Thread Safety

Not thread-safe. Designed for single-threaded asyncio use. For multi-threaded access, wrap in locks or run each node in its own event loop.

### Backpressure

Datagram dispatch queue limits to 64 concurrent tasks. Excess packets are dropped with a warning log.

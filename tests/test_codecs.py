import pytest

from kademlia import (
    json_encode,
    json_decode,
    bencode_encode,
    bencode_decode,
    _BYTES_MARKER,
)

CODECS = [
    ("json", json_encode, json_decode),
    ("bencode", bencode_encode, bencode_decode),
]


@pytest.mark.parametrize("name,encode,decode", CODECS)
def test_str_roundtrip(name, encode, decode):
    msg = {"type": "set", "key": "hello", "value": "world"}
    assert decode(encode(msg)) == msg


@pytest.mark.parametrize("name,encode,decode", CODECS)
def test_bytes_roundtrip(name, encode, decode):
    msg = {"type": "set", "key": "blob", "value": b"\x00\x01\xff binary"}
    out = decode(encode(msg))
    assert out["value"] == b"\x00\x01\xff binary"
    assert isinstance(out["value"], bytes)


@pytest.mark.parametrize("name,encode,decode", CODECS)
def test_nested_structures(name, encode, decode):
    msg = {
        "type": "find_node_res",
        "id": "abc123",
        "peers": [
            {"node_id": "aa" * 20, "ip": "127.0.0.1", "port": 8000},
            {"node_id": "bb" * 20, "ip": "127.0.0.1", "port": 8001},
        ],
    }
    assert decode(encode(msg)) == msg


@pytest.mark.parametrize("name,encode,decode", CODECS)
def test_int_roundtrip(name, encode, decode):
    msg = {"type": "check_store", "size": 12345}
    assert decode(encode(msg))["size"] == 12345


@pytest.mark.parametrize("name,encode,decode", CODECS)
def test_bool_truthiness_preserved(name, encode, decode):
    # bencode has no bool type; True/False decode as 1/0 — truthiness must survive
    out_true = decode(encode({"has_key": True}))
    out_false = decode(encode({"has_key": False}))
    assert bool(out_true["has_key"]) is True
    assert bool(out_false["has_key"]) is False


@pytest.mark.parametrize("name,encode,decode", CODECS)
def test_none_values_stripped(name, encode, decode):
    msg = {"type": "get_res", "id": "x", "value": None}
    out = decode(encode(msg))
    assert "value" not in out
    assert out["type"] == "get_res"


@pytest.mark.parametrize("name,encode,decode", CODECS)
def test_empty_string_kept(name, encode, decode):
    out = decode(encode({"value": ""}))
    assert out["value"] == ""


def test_json_bytes_marker_dict_shape():
    encoded = json_encode({"value": b"data"})
    assert b"__bytes_b64__" in encoded
    assert json_decode(encoded)["value"] == b"data"


def test_bencode_marker_collision_documented():
    # Known limitation: a str starting with the internal bytes marker
    # round-trips as bytes. Documented in README Design Notes.
    tricky = _BYTES_MARKER.decode("latin-1") + "hello"
    out = bencode_decode(bencode_encode({"value": tricky}))
    assert out["value"] == b"hello"


def test_bencode_rejects_trailing_data():
    with pytest.raises(ValueError):
        bencode_decode(bencode_encode({"a": 1}) + b"garbage")

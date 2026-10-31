"""Wire-format behavior, independent of server command handling."""

import asyncio

import pytest

from minikv.resp import Error, ProtocolError, SimpleString, encode, read


@pytest.mark.parametrize(
    "value,wire",
    [
        (SimpleString("OK"), b"+OK\r\n"),
        (Error("ERR nope"), b"-ERR nope\r\n"),
        (0, b":0\r\n"),
        (-42, b":-42\r\n"),
        (None, b"$-1\r\n"),
        (b"", b"$0\r\n\r\n"),
        (b"\x00\xff\r\n", b"$4\r\n\x00\xff\r\n\r\n"),
        ([], b"*0\r\n"),
        ([b"GET", [1, None]], b"*2\r\n$3\r\nGET\r\n*2\r\n:1\r\n$-1\r\n"),
    ],
)
def test_encode_and_parse(value, wire):
    assert encode(value) == wire

    async def scenario():
        reader = asyncio.StreamReader()
        reader.feed_data(wire)
        reader.feed_eof()
        assert await read(reader) == value
        with pytest.raises(EOFError):
            await read(reader)

    asyncio.run(scenario())


def test_fragmented_and_pipelined_frames():
    async def scenario():
        reader = asyncio.StreamReader()
        task = asyncio.create_task(read(reader))
        for chunk in (b"*1\r", b"\n$4\r\nP", b"ING\r", b"\n:2\r\n"):
            reader.feed_data(chunk)
            await asyncio.sleep(0)
        assert await task == [b"PING"]
        assert await read(reader) == 2
        reader.feed_data(b"*-1\r\n")
        assert await read(reader) is None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "wire",
    [
        b"?bad\r\n", b":x\r\n", b":+1\r\n", b": 1\r\n", b":\r\n",
        b"$-2\r\n", b"*-2\r\n", b"$1\r\naXX", b"$4\r\nabc",
        b"*1\r\n", b"+unfinished", b"+bad\nline\r\n", b"+\xff\r\n",
        b"$1048577\r\n", b"*1025\r\n", b"*1\r\n" * 34 + b":1\r\n",
        b"+" + b"x" * 65537 + b"\r\n",
    ],
)
def test_malformed_or_oversized_frames(wire):
    async def scenario():
        reader = asyncio.StreamReader()
        reader.feed_data(wire)
        reader.feed_eof()
        with pytest.raises(ProtocolError):
            await read(reader)

    asyncio.run(scenario())


@pytest.mark.parametrize("value", [SimpleString("bad\r\nreply"), Error("bad\nreply")])
def test_encoder_rejects_line_injection(value):
    with pytest.raises(ValueError):
        encode(value)


def test_encoder_rejects_unsupported_values():
    with pytest.raises(TypeError):
        encode({"key": "value"})

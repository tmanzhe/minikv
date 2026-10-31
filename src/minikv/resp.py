"""RESP2 framing for asyncio streams."""

import asyncio
from dataclasses import dataclass


class ProtocolError(ValueError):
    """The input is not a valid or supported RESP2 frame."""


@dataclass(frozen=True)
class SimpleString:
    value: str


@dataclass(frozen=True)
class Error:
    value: str


def encode(value):
    """Encode a RESP2 reply. None represents a null bulk string."""
    if isinstance(value, (SimpleString, Error)):
        text = value.value.encode("utf-8")
        if b"\r" in text or b"\n" in text:
            raise ValueError("RESP line replies cannot contain newlines")
        return (b"+" if isinstance(value, SimpleString) else b"-") + text + b"\r\n"
    if value is None:
        return b"$-1\r\n"
    if type(value) is int:
        return b":" + str(value).encode("ascii") + b"\r\n"
    if isinstance(value, bytes):
        return b"$" + str(len(value)).encode("ascii") + b"\r\n" + value + b"\r\n"
    if isinstance(value, (list, tuple)):
        return b"*" + str(len(value)).encode("ascii") + b"\r\n" + b"".join(map(encode, value))
    raise TypeError("unsupported RESP value")


def _number(raw):
    digits = raw[1:] if raw.startswith(b"-") else raw
    if not digits or not digits.isdigit():
        raise ProtocolError("invalid integer")
    try:
        return int(raw)
    except ValueError as exc:
        raise ProtocolError("invalid integer") from exc


async def read(reader: asyncio.StreamReader, *, _depth=0):
    """Read one frame, leaving pipelined frames in the stream.

    EOF between frames raises EOFError; truncated frames raise ProtocolError.
    Bulk strings are limited to 1 MiB, arrays to 1024 items and nesting to 32.
    """
    if _depth > 32:
        raise ProtocolError("nesting too deep")
    try:
        prefix = await reader.readexactly(1)
    except asyncio.IncompleteReadError as exc:
        raise EOFError from exc
    try:
        raw = (await reader.readuntil(b"\r\n"))[:-2]
        if b"\r" in raw or b"\n" in raw:
            raise ProtocolError("invalid line")
        if prefix == b"+":
            return SimpleString(raw.decode("utf-8"))
        if prefix == b"-":
            return Error(raw.decode("utf-8"))
        if prefix == b":":
            return _number(raw)
        if prefix not in (b"$", b"*"):
            raise ProtocolError("unknown RESP type")
        length = _number(raw)
        if length == -1:
            return None
        if length < 0:
            raise ProtocolError("invalid length")
        if prefix == b"$":
            if length > 1024 * 1024:
                raise ProtocolError("bulk string too large")
            data = await reader.readexactly(length + 2)
            if data[-2:] != b"\r\n":
                raise ProtocolError("invalid bulk string terminator")
            return data[:-2]
        if length > 1024:
            raise ProtocolError("array too large")
        return [await read(reader, _depth=_depth + 1) for _ in range(length)]
    except (asyncio.IncompleteReadError, EOFError, asyncio.LimitOverrunError, UnicodeError) as exc:
        raise ProtocolError("invalid or incomplete frame") from exc

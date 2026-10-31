"""Exercise the installed server over real local TCP connections."""

import asyncio
import subprocess
import sys

import pytest

from minikv.resp import Error, SimpleString, encode, read


def test_commands_pipeline_and_shared_state(server_port):
    async def scenario():
        reader, writer = await asyncio.open_connection("127.0.0.1", server_port)
        commands = [
            [b"pInG"], [b"PING", b"hi"], [b"ECHO", b"\x00\xff\r\n"],
            [b"GET", b"missing"], [b"SET", b"", b""], [b"GET", b""],
            [b"SET", b"\xff", b"first"], [b"SET", b"\xff", b"\x00\xff\r\n"],
            [b"GET", b"\xff"], [b"SET", b"other", b"value"],
            [b"EXISTS", b"\xff", b"other", b"\xff", b"missing"],
            [b"DEL", b"\xff", b"\xff", b"missing", b"other"],
            [b"EXISTS", b"\xff", b"other"], [b"GET", b"\xff"],
        ]
        expected = [
            SimpleString("PONG"), b"hi", b"\x00\xff\r\n", None,
            SimpleString("OK"), b"", SimpleString("OK"), SimpleString("OK"),
            b"\x00\xff\r\n", SimpleString("OK"), 3, 2, 0, None,
        ]
        try:
            writer.write(b"".join(encode(command) for command in commands))
            await writer.drain()
            for reply in expected:
                assert await asyncio.wait_for(read(reader), 3) == reply
            other_reader, other_writer = await asyncio.open_connection("127.0.0.1", server_port)
            try:
                other_writer.write(encode([b"GET", b""]))
                await other_writer.drain()
                assert await asyncio.wait_for(read(other_reader), 3) == b""
            finally:
                other_writer.close()
                await other_writer.wait_closed()
        finally:
            writer.close()
            await writer.wait_closed()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "command",
    [[b"PING", b"a", b"b"], [b"ECHO"], [b"GET"], [b"GET", b"a", b"b"],
     [b"SET", b"a"], [b"SET", b"a", b"b", b"extra"], [b"DEL"], [b"EXISTS"]],
)
def test_wrong_arity_keeps_connection_usable(server_port, command):
    async def scenario():
        reader, writer = await asyncio.open_connection("127.0.0.1", server_port)
        try:
            writer.write(encode(command) + encode([b"PING"]))
            await writer.drain()
            error = await asyncio.wait_for(read(reader), 3)
            assert isinstance(error, Error)
            assert error.value.startswith("ERR wrong number of arguments")
            assert await asyncio.wait_for(read(reader), 3) == SimpleString("PONG")
        finally:
            writer.close()
            await writer.wait_closed()

    asyncio.run(scenario())


@pytest.mark.parametrize("command", [[b"unknown\r\ncommand"], [b"\xff"], [], [None], b"PING", [1]])
def test_invalid_commands_return_errors(server_port, command):
    async def scenario():
        reader, writer = await asyncio.open_connection("127.0.0.1", server_port)
        try:
            writer.write(encode(command) + encode([b"PING"]))
            await writer.drain()
            error = await asyncio.wait_for(read(reader), 3)
            assert isinstance(error, Error)
            assert error.value.startswith("ERR ")
            assert await asyncio.wait_for(read(reader), 3) == SimpleString("PONG")
        finally:
            writer.close()
            await writer.wait_closed()

    asyncio.run(scenario())


@pytest.mark.parametrize("wire", [b"?bad\r\n", b"$1\r\naXX", b"*1\r\n$4\r\nPI"])
def test_protocol_error_closes_connection(server_port, wire):
    async def scenario():
        reader, writer = await asyncio.open_connection("127.0.0.1", server_port)
        try:
            writer.write(wire)
            await writer.drain()
            writer.write_eof()
            error = await asyncio.wait_for(read(reader), 3)
            assert isinstance(error, Error)
            assert error.value.startswith("ERR Protocol error:")
            assert await asyncio.wait_for(reader.read(), 3) == b""
        finally:
            writer.close()
            await writer.wait_closed()

    asyncio.run(scenario())


def test_python_module_cli():
    result = subprocess.run(
        [sys.executable, "-m", "minikv", "--help"],
        capture_output=True, text=True, timeout=5,
    )
    assert result.returncode == 0
    assert "--port" in result.stdout


def test_cli_rejects_invalid_port():
    result = subprocess.run(
        [sys.executable, "-m", "minikv", "--port", "65536"],
        capture_output=True, text=True, timeout=5,
    )
    assert result.returncode == 2
    assert "port must be between 0 and 65535" in result.stderr

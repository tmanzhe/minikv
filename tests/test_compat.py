"""Compatibility with the real redis-cli executable, using only localhost."""

import shutil
import subprocess

import pytest


REDIS_CLI = shutil.which("redis-cli")


@pytest.mark.skipif(REDIS_CLI is None, reason="redis-cli is not installed or is not on PATH")
def test_redis_cli_commands(server_port):
    def cli(*args):
        result = subprocess.run(
            [REDIS_CLI, "-2", "-h", "127.0.0.1", "-p", str(server_port), "--raw", *args],
            capture_output=True, timeout=5,
        )
        assert result.returncode == 0, result.stderr
        assert result.stderr == b""
        return result.stdout

    assert cli("PING") == b"PONG\n"
    assert cli("PING", "hello") == b"hello\n"
    assert cli("ECHO", "hello world") == b"hello world\n"
    assert cli("GET", "missing") == b"\n"
    assert cli("SET", "one", "first") == b"OK\n"
    assert cli("SET", "one", "replacement") == b"OK\n"
    assert cli("GET", "one") == b"replacement\n"
    assert cli("SET", "two", "second") == b"OK\n"
    assert cli("EXISTS", "one", "two", "one", "missing") == b"3\n"
    assert cli("DEL", "one", "one", "two", "missing") == b"2\n"
    assert cli("EXISTS", "one", "two") == b"0\n"
    assert cli("GET", "one") == b"\n"
    assert cli("GET").startswith(b"ERR wrong number of arguments")
    assert cli("NO_SUCH_COMMAND").startswith(b"ERR unknown command")

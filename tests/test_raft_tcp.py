"""Three installed CLI processes communicating over real localhost TCP."""

import asyncio
from pathlib import Path
import socket
import subprocess
import sys

import pytest

from minikv.resp import Error, SimpleString, encode, read


async def call(address, *command):
    host, port = address.rsplit(":", 1)
    async with asyncio.timeout(6):
        reader, writer = await asyncio.open_connection(host, int(port))
        try:
            writer.write(encode(list(command)))
            await writer.drain()
            return await read(reader)
        finally:
            writer.close()
            await writer.wait_closed()


class Processes:
    def __init__(self, directory):
        self.directory = directory
        self.processes = {}
        reservations = [socket.socket() for _ in range(3)]
        try:
            for sock in reservations:
                sock.bind(("127.0.0.1", 0))
            self.addresses = [f"127.0.0.1:{sock.getsockname()[1]}" for sock in reservations]
        finally:
            for sock in reservations:
                sock.close()

    def start(self, address):
        index = self.addresses.index(address)
        command = [str(Path(sys.executable).with_name("minikv")),
                   "--port", address.rsplit(":", 1)[1],
                   "--data-dir", str(self.directory / str(index)), "--seed", str(index)]
        for peer in self.addresses:
            if peer != address:
                command += ["--peer", peer]
        self.processes[address] = subprocess.Popen(
            command, cwd=self.directory, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )

    def stop(self, address):
        process = self.processes.pop(address)
        process.kill()
        process.wait(timeout=5)
        process.stdout.close()

    async def leader(self):
        async with asyncio.timeout(20):
            while True:
                for address, process in list(self.processes.items()):
                    assert process.poll() is None, process.stdout.read().decode()
                    try:
                        if await call(address, b"PING") == SimpleString("PONG"):
                            return address
                    except (OSError, EOFError, TimeoutError):
                        pass
                await asyncio.sleep(0.05)


@pytest.fixture
def processes(tmp_path):
    cluster = Processes(tmp_path)
    try:
        yield cluster
    finally:
        for address in list(cluster.processes):
            cluster.stop(address)


def test_tcp_redirect_failover_and_quorum_reads(processes):
    async def scenario():
        for address in processes.addresses:
            processes.start(address)
        leader = await processes.leader()
        assert await call(leader, b"SET", b"key", b"survives") == SimpleString("OK")
        followers = [address for address in processes.addresses if address != leader]
        for follower in followers:
            for command in [(b"GET", b"key"), (b"SET", b"key", b"wrong")]:
                assert await call(follower, *command) == Error(f"MOVED {leader}")
        processes.stop(leader)
        new = await processes.leader()
        assert new != leader
        assert await call(new, b"GET", b"key") == b"survives"
        assert await call(new, b"SET", b"new", b"leader") == SimpleString("OK")
        processes.stop(next(address for address in followers if address != new))
        assert await call(new, b"GET", b"key") == Error("ERR no quorum; outcome unknown")
        assert await call(new, b"SET", b"key", b"isolated") == Error("ERR no quorum; outcome unknown")

    asyncio.run(scenario())


def test_tcp_restarted_follower_catches_up_and_can_lead(processes):
    async def scenario():
        # One node cannot elect itself and reports the absence of a leader.
        first = processes.addresses[0]
        processes.start(first)
        async with asyncio.timeout(10):
            while True:
                try:
                    assert await call(first, b"GET", b"key") == Error("ERR no leader")
                    break
                except OSError:
                    await asyncio.sleep(0.05)
        for address in processes.addresses[1:]:
            processes.start(address)
        leader = await processes.leader()
        laggard, standby = [address for address in processes.addresses if address != leader]
        processes.stop(laggard)
        assert await call(leader, b"SETIFNEWER", b"\x00\xff", b"value\r\n", b"9") == 1
        assert await call(leader, b"INVALIDATE", b"marker", b"20") == 1
        processes.start(laggard)
        processes.stop(standby)
        # Acknowledgement now requires the restarted follower to catch up.
        assert await call(leader, b"SET", b"fence", b"caught-up") == SimpleString("OK")
        processes.stop(leader)
        processes.start(standby)
        assert await processes.leader() == laggard
        assert await call(laggard, b"GETVERSIONED", b"\x00\xff") == [b"value\r\n", 9]
        assert await call(laggard, b"GET", b"fence") == b"caught-up"
        assert await call(laggard, b"SETIFNEWER", b"marker", b"stale", b"19") == 0

    asyncio.run(scenario())

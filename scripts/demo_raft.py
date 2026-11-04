#!/usr/bin/env python3
"""Run three local CLI nodes, demonstrate failover, and clean up on exit."""

import asyncio
from pathlib import Path
import socket
import subprocess
import sys
import tempfile

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


async def find_leader(processes):
    async with asyncio.timeout(20):
        while True:
            for address, process in processes.items():
                if process.poll() is not None:
                    raise RuntimeError(f"node {address} exited: {process.stdout.read().decode()}")
                try:
                    if await call(address, b"PING") == SimpleString("PONG"):
                        return address
                except (OSError, EOFError, TimeoutError):
                    pass
            await asyncio.sleep(0.05)


def stop(process):
    process.kill()
    process.wait(timeout=5)
    process.stdout.close()


async def demo():
    processes = {}
    with tempfile.TemporaryDirectory(prefix="minikv-raft-") as directory:
        reservations = [socket.socket() for _ in range(3)]
        try:
            for sock in reservations:
                sock.bind(("127.0.0.1", 0))
            addresses = [f"127.0.0.1:{sock.getsockname()[1]}" for sock in reservations]
        finally:
            for sock in reservations:
                sock.close()
        try:
            for index, address in enumerate(addresses):
                command = [sys.executable, "-m", "minikv", "--port", address.rsplit(":", 1)[1],
                           "--data-dir", str(Path(directory) / str(index)), "--seed", str(index)]
                for peer in addresses:
                    if peer != address:
                        command += ["--peer", peer]
                processes[address] = subprocess.Popen(
                    command, cwd=directory, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                )
            print("Started 3 local nodes: " + ", ".join(addresses), flush=True)
            leader = await find_leader(processes)
            print(f"Leader: {leader}", flush=True)
            reply = await call(leader, b"SET", b"hello", b"world")
            assert reply == SimpleString("OK"), reply
            print("SET hello world -> OK (committed)", flush=True)
            follower = next(address for address in addresses if address != leader)
            reply = await call(follower, b"GET", b"hello")
            assert reply == Error(f"MOVED {leader}"), reply
            print(f"Follower {follower}: GET hello -> {reply.value}", flush=True)
            stop(processes.pop(leader))
            print(f"Killed leader: {leader}", flush=True)
            new = await find_leader(processes)
            print(f"New leader: {new}", flush=True)
            reply = await call(new, b"GET", b"hello")
            assert reply == b"world", reply
            print("GET hello -> world (survived leader loss)", flush=True)
            reply = await call(new, b"SET", b"after", b"failover")
            assert reply == SimpleString("OK"), reply
            print("SET after failover -> OK (committed)", flush=True)
        finally:
            for process in processes.values():
                stop(process)
            print("Cleaned up all node processes and temporary data.", flush=True)


if __name__ == "__main__":
    asyncio.run(demo())

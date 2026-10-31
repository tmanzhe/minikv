"""Version ordering through the installed server's public TCP commands."""

import asyncio
from itertools import permutations
import random

import pytest

from minikv.resp import Error, SimpleString, encode, read


class Client:
    async def connect(self, port):
        self.reader, self.writer = await asyncio.open_connection("127.0.0.1", port)
        return self

    async def call(self, *command):
        self.writer.write(encode(list(command)))
        await self.writer.drain()
        return await asyncio.wait_for(read(self.reader), 3)

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()


def test_versioned_commands_and_legacy_interactions(server_port):
    async def scenario():
        client = await Client().connect(server_port)
        key = b"\x00\xff\r\n"
        try:
            assert await client.call(b"GETVERSIONED", key) is None
            assert await client.call(b"SETIFNEWER", key, b"", b"0") == 1
            assert await client.call(b"GETVERSIONED", key) == [b"", 0]
            assert await client.call(b"EXISTS", key, key) == 2
            assert await client.call(b"SETIFNEWER", key, b"conflict", b"0") == 0
            assert await client.call(b"SETIFNEWER", key, b"new", b"2") == 1
            assert await client.call(b"INVALIDATE", key, b"1") == 0
            assert await client.call(b"GET", key) == b"new"
            assert await client.call(b"INVALIDATE", key, b"2") == 1
            assert await client.call(b"INVALIDATE", key, b"2") == 0
            assert await client.call(b"GETVERSIONED", key) is None
            assert await client.call(b"GET", key) is None
            assert await client.call(b"EXISTS", key) == 0
            assert await client.call(b"DEL", key) == 0
            assert await client.call(b"SETIFNEWER", key, b"old", b"2") == 0
            assert await client.call(b"SET", key, b"legacy") == SimpleString("OK")
            assert await client.call(b"GETVERSIONED", key) == [b"legacy", 3]
            assert await client.call(b"DEL", key, key) == 1
            assert await client.call(b"SETIFNEWER", key, b"old", b"3") == 0
            assert await client.call(b"SETIFNEWER", key, b"fresh", b"4") == 1
            assert await client.call(b"GETVERSIONED", key) == [b"fresh", 4]
            assert await client.call(b"INVALIDATE", b"absent", b"10") == 1
            assert await client.call(b"SETIFNEWER", b"absent", b"old", b"9") == 0
            assert await client.call(b"GET", key) == b"fresh"
        finally:
            await client.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("raw", [b"", b"-1", b"+1", b"1.0", b" 1", b"1\n", b"\xff",
                                   b"9223372036854775808", b"9" * 5000])
def test_bad_versions_leave_state_and_connection_usable(server_port, raw):
    async def scenario():
        client = await Client().connect(server_port)
        try:
            assert await client.call(b"SETIFNEWER", b"key", b"original", b"7") == 1
            for command in [(b"SETIFNEWER", b"key", b"bad", raw),
                            (b"INVALIDATE", b"key", raw)]:
                reply = await client.call(*command)
                assert isinstance(reply, Error)
                assert reply.value.startswith("ERR version must be")
                assert await client.call(b"GETVERSIONED", b"key") == [b"original", 7]
        finally:
            await client.close()

    asyncio.run(scenario())


def test_maximum_version(server_port):
    async def scenario():
        client = await Client().connect(server_port)
        try:
            maximum = b"9223372036854775807"
            assert await client.call(b"SETIFNEWER", b"key", b"last", maximum) == 1
            assert await client.call(b"SET", b"key", b"overflow") == Error("ERR version exhausted")
            assert await client.call(b"GETVERSIONED", b"key") == [b"last", int(maximum)]
            assert await client.call(b"INVALIDATE", b"key", maximum) == 1
            assert await client.call(b"SETIFNEWER", b"key", b"late", maximum) == 0
        finally:
            await client.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("command", [[b"GETVERSIONED"], [b"GETVERSIONED", b"k", b"extra"],
                                     [b"SETIFNEWER", b"k", b"v"],
                                     [b"SETIFNEWER", b"k", b"v", b"1", b"extra"],
                                     [b"INVALIDATE", b"k"], [b"INVALIDATE", b"k", b"1", b"extra"]])
def test_versioned_command_arity(server_port, command):
    async def scenario():
        client = await Client().connect(server_port)
        try:
            reply = await client.call(*command)
            assert isinstance(reply, Error)
            assert reply.value.startswith("ERR wrong number of arguments")
            assert await client.call(b"PING") == SimpleString("PONG")
        finally:
            await client.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("invalidate", [False, True])
def test_slow_fill_after_acknowledged_update(server_port, invalidate):
    async def scenario():
        fill, update = await Client().connect(server_port), await Client().connect(server_port)
        captured, release = asyncio.Event(), asyncio.Event()

        async def slow_fill():
            # Stand-in for a backend read that captured v1 before the update.
            value, version = b"old", b"1"
            captured.set()
            await release.wait()
            return await fill.call(b"SETIFNEWER", b"key", value, version)

        pending = asyncio.create_task(slow_fill())
        try:
            await asyncio.wait_for(captured.wait(), 3)
            assert await update.call(b"SETIFNEWER", b"key", b"new", b"2") == 1
            if invalidate:
                assert await update.call(b"INVALIDATE", b"key", b"2") == 1
            release.set()
            assert await asyncio.wait_for(pending, 3) == 0
            expected = None if invalidate else [b"new", 2]
            assert await update.call(b"GETVERSIONED", b"key") == expected
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            await fill.close()
            await update.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("seed", range(16))
def test_randomized_fill_update_invalidate_races(server_port, seed):
    async def scenario():
        rng = random.Random(seed)
        clients = [await Client().connect(server_port) for _ in range(3)]
        state = (None, -1)
        try:
            for round_number in range(64):
                floor = state[1]
                latest = max(0, floor + rng.randint(1, 4))
                old = rng.randint(0, latest)
                invalidation = max(0, latest + rng.randint(-2, 2))
                operations = [
                    (b"SETIFNEWER", f"fill-{round_number}".encode(), old),
                    (b"SETIFNEWER", f"update-{round_number}".encode(), latest),
                    (b"INVALIDATE", None, invalidation),
                ]
                rng.shuffle(operations)
                delays = [rng.random() / 1000 for _ in operations]
                events, replies = [], [None] * 3

                async def run(index):
                    await asyncio.sleep(delays[index])
                    command, value, version = operations[index]
                    args = [command, b"key"]
                    if value is not None:
                        args.append(value)
                    args.append(str(version).encode())
                    events.append((index, "start"))
                    replies[index] = await clients[index].call(*args)
                    events.append((index, "end"))

                await asyncio.gather(*(run(i) for i in range(3)))
                observed = await clients[0].call(b"GETVERSIONED", b"key")
                candidates = []
                # Check a legal serial history, including real-time precedence.
                for order in permutations(range(3)):
                    if any(events.index((a, "end")) < events.index((b, "start"))
                           and order.index(a) > order.index(b)
                           for a in range(3) for b in range(3) if a != b):
                        continue
                    value, version = state
                    expected = [None] * 3
                    for i in order:
                        command, proposed, incoming = operations[i]
                        if command == b"INVALIDATE":
                            accepted = incoming >= version and (incoming > version or value is not None)
                        else:
                            accepted = incoming > version
                        expected[i] = int(accepted)
                        if accepted:
                            value, version = proposed, incoming
                    visible = None if value is None else [value, version]
                    if replies == expected and observed == visible:
                        candidates.append((value, version))
                context = (seed, round_number, state, operations, events, replies, observed)
                assert candidates, context
                assert all(candidate == candidates[0] for candidate in candidates), context
                state = candidates[0]
                assert state[1] == max(floor, latest, invalidation), context
                # Even after invalidation, an acknowledged version cannot regress.
                assert await clients[0].call(
                    b"SETIFNEWER", b"key", b"stale-probe", str(state[1]).encode()
                ) == 0, context
                assert await clients[0].call(b"GETVERSIONED", b"key") == observed, context
        finally:
            for client in clients:
                await client.close()

    asyncio.run(scenario())

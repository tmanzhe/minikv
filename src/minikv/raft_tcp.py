"""Asyncio adapter for the clock-driven core, using the existing RESP listener."""

import asyncio

from .raft import Entry, Raft
from .resp import Error, ProtocolError, SimpleString, encode, read
from .server import execute, start_server
from .store import Store


def split_address(address):
    host, separator, port = address.rpartition(":")
    if (not separator or not host or any(c.isspace() for c in address)
            or not port.isascii() or not port.isdigit() or not 1 <= int(port) <= 65535):
        raise ValueError("node address must be host:port with port between 1 and 65535")
    return host, int(port)


def pack(source, message):
    kind = message["kind"]
    fields = {
        "vote": ("last_index", "last_term"),
        "vote_reply": ("granted",),
        "append": ("prev_index", "prev_term", "entries", "leader_commit"),
        "append_reply": ("success", "prev_index", "match"),
    }[kind]
    values = []
    for field in fields:
        value = message[field]
        if field == "entries":
            value = [[entry.term, entry.command] for entry in value]
        elif isinstance(value, bool):
            value = int(value)
        values.append(value)
    return [b"RAFT", source.encode(), kind.encode(), message["term"], *values]


def unpack(request):
    """Validate peer frames before letting them change Raft state."""
    try:
        _, raw_source, raw_kind, term, *values = request
        source, kind = raw_source.decode(), raw_kind.decode()
        fields = {
            "vote": ("last_index", "last_term"),
            "vote_reply": ("granted",),
            "append": ("prev_index", "prev_term", "entries", "leader_commit"),
            "append_reply": ("success", "prev_index", "match"),
        }[kind]
        if type(term) is not int or term < 0 or len(fields) != len(values):
            raise ValueError
        message = dict(zip(fields, values))
        for field, value in message.items():
            if field == "entries":
                if not isinstance(value, list) or len(value) > 32:
                    raise ValueError
                entries = []
                for entry_term, command in value:
                    if (type(entry_term) is not int or not 0 < entry_term <= term or
                            (command is not None and (not isinstance(command, list) or
                             not command or any(not isinstance(arg, bytes) for arg in command)))):
                        raise ValueError
                    entries.append(Entry(entry_term, command))
                message[field] = entries
            elif type(value) is not int or value < 0:
                raise ValueError
            elif field in ("success", "granted") and value not in (0, 1):
                raise ValueError
        return source, {"kind": kind, "term": term, **message}
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise ProtocolError("invalid Raft message") from exc


class RaftServer:
    def __init__(self, host, port, peers, *, state_path=None, seed=None, request_timeout=3):
        self.host, self.port = host, port
        self.store = Store()
        self.changed = asyncio.Event()
        self.tasks = set()
        self.connections = set()
        self.request_timeout = request_timeout
        self.node = Raft(f"{host}:{port}", peers,
                         clock=asyncio.get_running_loop().time, send=self.send,
                         apply=lambda request: execute(request, self.store),
                         state_path=state_path, seed=seed)

    async def start(self):
        self.server = await start_server(self.host, self.port, raft=self)
        self.ticker = asyncio.create_task(self._tick())
        return self

    async def _tick(self):
        while True:
            self.node.tick()
            self.changed.set()
            await asyncio.sleep(0.02)

    def send(self, source, target, message):
        task = asyncio.create_task(self._send(target, pack(source, message)))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def _send(self, target, request):
        writer = None
        try:
            async with asyncio.timeout(0.5):
                reader, writer = await asyncio.open_connection(*split_address(target))
                writer.write(encode(request))
                await writer.drain()
                await read(reader)
        except (OSError, TimeoutError, EOFError, ProtocolError):
            # Heartbeats retry failed or lost messages; no network failure is a vote.
            pass
        finally:
            if writer is not None:
                writer.close()
                try:
                    async with asyncio.timeout(0.5):
                        await writer.wait_closed()
                except (OSError, TimeoutError):
                    pass

    def receive(self, request):
        source, message = unpack(request)
        if source not in self.node.peers:
            return Error("ERR unknown Raft peer")
        self.node.receive(source, message)
        self.changed.set()
        return SimpleString("OK")

    def redirect(self):
        if self.node.leader is not None:
            return Error(f"MOVED {self.node.leader}")
        return Error("ERR no leader")

    async def execute(self, request):
        if (not isinstance(request, list) or not request or
                any(not isinstance(arg, bytes) for arg in request)):
            return Error("ERR command must be a non-empty array of bulk strings")
        if self.node.role != "leader":
            return self.redirect()
        index, term = self.node.propose(request)
        try:
            async with asyncio.timeout(self.request_timeout):
                while True:
                    self.changed.clear()
                    if index <= self.node.applied and self.node.log[index].term == term:
                        return self.node.results[index]
                    if self.node.role != "leader" or self.node.term != term:
                        return self.redirect()
                    await self.changed.wait()
        except TimeoutError:
            return Error("ERR no quorum; outcome unknown")

    async def close(self):
        self.server.close()
        await self.server.wait_closed()
        self.ticker.cancel()
        for task in [*self.tasks, *self.connections]:
            task.cancel()
        await asyncio.gather(self.ticker, *self.tasks, *self.connections, return_exceptions=True)

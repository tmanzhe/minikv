"""Asyncio TCP server and command-line entry point."""

import argparse
import asyncio

from .resp import Error, ProtocolError, SimpleString, encode, read
from .store import MAX_VERSION, Store


def execute(request, store):
    if not isinstance(request, list) or not request or any(
        not isinstance(part, bytes) for part in request
    ):
        return Error("ERR command must be a non-empty array of bulk strings")
    command, *args = request
    command = command.upper()
    arities = {
        b"PING": (0, 1), b"ECHO": (1,), b"GET": (1,), b"SET": (2,),
        b"GETVERSIONED": (1,), b"SETIFNEWER": (3,), b"INVALIDATE": (2,),
    }
    if command in arities:
        valid = len(args) in arities[command]
    elif command in (b"DEL", b"EXISTS"):
        valid = bool(args)
    else:
        return Error("ERR unknown command")
    if not valid:
        return Error(f"ERR wrong number of arguments for '{command.decode('ascii').lower()}' command")
    if command == b"PING":
        return args[0] if args else SimpleString("PONG")
    if command == b"ECHO":
        return args[0]
    if command == b"GET":
        return store.get(args[0])
    if command == b"GETVERSIONED":
        return store.get_versioned(args[0])
    if command in (b"SETIFNEWER", b"INVALIDATE"):
        raw_version = args[-1]
        try:
            if not raw_version.isdigit():
                raise ValueError
            version = int(raw_version)
            if version > MAX_VERSION:
                raise ValueError
        except ValueError:
            return Error("ERR version must be an integer between 0 and 9223372036854775807")
        if command == b"SETIFNEWER":
            return store.set_if_newer(args[0], args[1], version)
        return store.invalidate(args[0], version)
    if command == b"SET":
        try:
            store.set(args[0], args[1])
        except ValueError as exc:
            return Error(f"ERR {exc}")
        return SimpleString("OK")
    if command == b"EXISTS":
        return sum(store.get(key) is not None for key in args)
    return sum(store.delete(key) for key in args)


async def start_server(host="127.0.0.1", port=6379, *, raft=None):
    """Start a server with a shared, in-memory store for its clients."""
    store = Store()

    async def handle(reader, writer):
        task = asyncio.current_task()
        if raft is not None:
            raft.connections.add(task)
        try:
            while True:
                try:
                    request = await read(reader)
                    if raft is None:
                        reply = execute(request, store)
                    elif isinstance(request, list) and request and request[0] == b"RAFT":
                        reply = raft.receive(request)
                    else:
                        reply = await raft.execute(request)
                except EOFError:
                    break
                except ProtocolError as exc:
                    writer.write(encode(Error(f"ERR Protocol error: {exc}")))
                    await writer.drain()
                    break
                writer.write(encode(reply))
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            if raft is not None:
                raft.connections.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    return await asyncio.start_server(handle, host, port)


async def _serve(host, port, peers=(), data_dir=None, seed=None):
    if peers:
        from pathlib import Path
        from .raft_tcp import RaftServer

        cluster = await RaftServer(host, port, peers,
                                   state_path=Path(data_dir) / "raft.json", seed=seed).start()
        server = cluster.server
    else:
        cluster = None
        server = await start_server(host, port)
    try:
        async with server:
            address = server.sockets[0].getsockname()
            print(f"Listening on {address[0]}:{address[1]}", flush=True)
            await server.serve_forever()
    finally:
        if cluster is not None:
            await cluster.close()


def main():
    parser = argparse.ArgumentParser(description="Run the minikv in-memory RESP2 server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--peer", action="append", default=[], metavar="HOST:PORT",
                        help="Raft peer address; supply exactly twice for a three-node cluster")
    parser.add_argument("--data-dir", help="directory for this node's persistent Raft state")
    parser.add_argument("--seed", type=int, help="seed for randomized Raft election timeouts")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("port must be between 0 and 65535")
    if args.peer:
        from .raft_tcp import split_address

        try:
            for address in [f"{args.host}:{args.port}", *args.peer]:
                split_address(address)
            if len(args.peer) != 2 or len({f"{args.host}:{args.port}", *args.peer}) != 3:
                raise ValueError("Raft requires three distinct node addresses")
            if not args.data_dir:
                raise ValueError("--data-dir is required with --peer; use a separate directory per node")
        except ValueError as exc:
            parser.error(str(exc))
    elif args.data_dir or args.seed is not None:
        parser.error("--data-dir and --seed require --peer")
    try:
        asyncio.run(_serve(args.host, args.port, args.peer, args.data_dir, args.seed))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

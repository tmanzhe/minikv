"""Asyncio TCP server and command-line entry point."""

import argparse
import asyncio

from .resp import Error, ProtocolError, SimpleString, encode, read


def execute(request, store):
    if not isinstance(request, list) or not request or any(
        not isinstance(part, bytes) for part in request
    ):
        return Error("ERR command must be a non-empty array of bulk strings")
    command, *args = request
    command = command.upper()
    arities = {b"PING": (0, 1), b"ECHO": (1,), b"GET": (1,), b"SET": (2,)}
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
    if command == b"SET":
        store[args[0]] = args[1]
        return SimpleString("OK")
    if command == b"EXISTS":
        return sum(key in store for key in args)
    deleted = 0
    for key in args:
        if key in store:
            del store[key]
            deleted += 1
    return deleted


async def start_server(host="127.0.0.1", port=6379):
    """Start a server with a shared, in-memory store for its clients."""
    store = {}

    async def handle(reader, writer):
        try:
            while True:
                try:
                    request = await read(reader)
                except EOFError:
                    break
                except ProtocolError as exc:
                    writer.write(encode(Error(f"ERR Protocol error: {exc}")))
                    await writer.drain()
                    break
                writer.write(encode(execute(request, store)))
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    return await asyncio.start_server(handle, host, port)


async def _serve(host, port):
    server = await start_server(host, port)
    async with server:
        address = server.sockets[0].getsockname()
        print(f"Listening on {address[0]}:{address[1]}", flush=True)
        await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Run the minikv in-memory RESP2 server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("port must be between 0 and 65535")
    try:
        asyncio.run(_serve(args.host, args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

# minikv

A tiny, in-memory key-value server for Python 3.12+. It speaks RESP2 over TCP.
Data lives until the process stops.

```sh
uv sync
uv run minikv --port 6380
```

In another terminal:

```sh
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
```

Commands: `PING [message]`, `ECHO message`, `GET key`, `SET key value`,
`DEL key [key ...]`, and `EXISTS key [key ...]`. Keys and values can contain
arbitrary bytes. `SET` replaces the value. Missing keys return null from `GET`.
`DEL` counts deleted keys; `EXISTS` counts matching arguments, including repeats.

The server defaults to `127.0.0.1:6379`. Use `--host` and `--port` to change it,
or run it with `python -m minikv`.
Bulk strings can hold up to 1 MiB; command arrays can hold up to 1024 items.

Build and test the installed package:

```sh
uv build
uv sync --no-editable
uv run --no-sync python -m pytest
```

The compatibility test uses a local `redis-cli` and skips if it isn't installed.

MIT licensed. See [LICENSE](LICENSE).

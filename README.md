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

## Versioned cache

Based on [Meta's cache race](https://engineering.fb.com/2022/06/08/core-infra/cache-made-consistent/).
`SETIFNEWER key value version` returns 1 if it installs the value, or 0 if
the version is equal to or older than the key's last version.
`GETVERSIONED key` returns `[value, version]`, or null if absent.
Versions are caller-supplied integers from 0 through 9223372036854775807.
Each command runs atomically in the server's event loop.

`INVALIDATE key version` drops values through that version and keeps a version
marker, even for a missing key. It returns 1 if it changes the key, otherwise 0.
An older invalidation leaves a newer value alone. A refill needs a strictly
higher version than the marker. `DEL` keeps the last version too. Plain `SET`
advances it by one, starting at 0, and returns an error if versions run out.

```text
slow fill             update client          cache
capture old value v1
                      SETIFNEWER new v2  ->  new v2
                      INVALIDATE v2      ->  empty, marker v2
SETIFNEWER old v1                        ->  rejected, still empty
```

The installed-wheel suite: 86 passed, 0 failed, 1 skipped (`redis-cli` missing).
16 fixed seeds cover 1,024 concurrent fill/update/invalidate races: 3,072 racing
commands plus 1,024 stale-fill probes, with zero stale overwrites. Two forced
slow-fill tests cover an acknowledged update with and without invalidation.
Markers stay in memory until restart. There is no backend or eviction here.

MIT licensed. See [LICENSE](LICENSE).

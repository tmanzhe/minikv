# minikv

A Python 3.12 project scaffold for a minimal key-value store. No server or
storage behavior is implemented yet.

Install [uv](https://docs.astral.sh/uv/), then run:

```sh
uv sync
uv run python -m pytest
```

Code lives in `src/minikv/`; behavior tests live in `tests/`.
To build and test the installed distribution:

```sh
uv build
uv sync --no-editable
uv run --no-sync python -m pytest
```

Licensed under the MIT license. See [LICENSE](LICENSE).

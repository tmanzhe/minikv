"""Start the installed CLI as a real subprocess on an OS-assigned port."""

import queue
import subprocess
import sys
import threading
from pathlib import Path

import pytest


@pytest.fixture
def server_port(tmp_path):
    executable = Path(sys.executable).with_name("minikv")
    process = subprocess.Popen(
        [str(executable), "--port", "0"],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines = queue.Queue()
    threading.Thread(target=lambda: lines.put(process.stdout.readline()), daemon=True).start()
    try:
        try:
            line = lines.get(timeout=5)
        except queue.Empty:
            pytest.fail("minikv did not announce its listening port within 5 seconds")
        assert line.startswith("Listening on 127.0.0.1:"), line
        yield int(line.rsplit(":", 1)[1])
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        process.stdout.close()

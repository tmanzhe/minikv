"""Smoke tests for the installed package."""

from importlib.metadata import version

import minikv


def test_installed_package_exposes_its_version():
    assert minikv.__version__ == version("minikv") == "0.1.0"

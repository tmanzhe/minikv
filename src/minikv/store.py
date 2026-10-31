"""Versioned values and retained invalidation markers for one server loop."""

MAX_VERSION = 2**63 - 1


class Store:
    """Mutations are synchronous, so each command is atomic in the event loop.

    A None value is a tombstone. Its version survives deletion until restart.
    """

    def __init__(self):
        self._entries = {}

    def get_versioned(self, key):
        value, version = self._entries.get(key, (None, -1))
        return None if value is None else [value, version]

    def get(self, key):
        return self._entries.get(key, (None, -1))[0]

    def set_if_newer(self, key, value, version):
        if version <= self._entries.get(key, (None, -1))[1]:
            return 0
        self._entries[key] = (value, version)
        return 1

    def set(self, key, value):
        version = self._entries.get(key, (None, -1))[1] + 1
        if version > MAX_VERSION:
            raise ValueError("version exhausted")
        self._entries[key] = (value, version)

    def invalidate(self, key, version):
        value, current = self._entries.get(key, (None, -1))
        if version < current or (version == current and value is None):
            return 0
        self._entries[key] = (None, version)
        return 1

    def delete(self, key):
        value, version = self._entries.get(key, (None, -1))
        if value is None:
            return 0
        self._entries[key] = (None, version)
        return 1

"""Clock-driven Raft for a fixed membership, with no threads or socket I/O.

Call tick after advancing the injected monotonic clock. The transport queues
messages; it must not call receive synchronously from send. All methods run on
one event loop. Log indices include a sentinel at zero.
"""

import base64
from collections import deque
from dataclasses import dataclass
import json
import os
from pathlib import Path
import random


@dataclass(frozen=True)
class Entry:
    term: int
    command: list[bytes] | None


class Raft:
    def __init__(self, address, peers, *, clock, send, apply, seed=None,
                 election=(0.45, 0.85), heartbeat=0.1, state_path=None):
        self.address = address
        self.peers = tuple(peers)
        if len(self.peers) != 2 or len({address, *self.peers}) != 3:
            raise ValueError("Raft requires three distinct node addresses")
        if not 0 < heartbeat < election[0] <= election[1]:
            raise ValueError("heartbeat must be shorter than election timeouts")
        self.clock, self.send, self.apply = clock, send, apply
        self.rng = random.Random(seed)
        self.election, self.heartbeat = election, heartbeat
        self.state_path = Path(state_path) if state_path is not None else None
        self.term, self.voted_for = 0, None
        self.log = [Entry(0, None)]
        if self.state_path is not None and self.state_path.exists():
            state = json.loads(self.state_path.read_text())
            self.term, self.voted_for = state["term"], state["voted_for"]
            self.log += [Entry(term, None if command is None else
                               [base64.b64decode(arg) for arg in command])
                         for term, command in state["log"]]
        self.role, self.leader = "follower", None
        self.commit_index = self.applied = 0
        self.results = {}
        self.votes = set()
        self.next_index, self.match_index = {}, {}
        self._reset_timeout()

    def _persist(self):
        if self.state_path is None:
            return
        state = {"term": self.term, "voted_for": self.voted_for,
                 "log": [[entry.term, None if entry.command is None else
                          [base64.b64encode(arg).decode("ascii") for arg in entry.command]]
                         for entry in self.log[1:]]}
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".tmp")
        with temporary.open("w") as stream:
            json.dump(state, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.state_path)
        directory = os.open(self.state_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def _reset_timeout(self):
        self.deadline = self.clock() + self.rng.uniform(*self.election)

    def _send(self, target, kind, **fields):
        self.send(self.address, target, {"kind": kind, "term": self.term, **fields})

    def _follow(self, term):
        if term > self.term:
            self.term, self.voted_for = term, None
            self._persist()
        self.role, self.leader = "follower", None
        self._reset_timeout()

    def tick(self):
        if self.role == "leader":
            if self.clock() >= self.next_heartbeat:
                self._broadcast()
        elif self.clock() >= self.deadline:
            self.role, self.leader = "candidate", None
            self.term += 1
            self.voted_for = self.address
            self.votes = {self.address}
            self._persist()
            self._reset_timeout()
            for peer in self.peers:
                self._send(peer, "vote", last_index=len(self.log) - 1,
                           last_term=self.log[-1].term)

    def receive(self, source, message):
        if source not in self.peers:
            return
        kind, term = message["kind"], message["term"]
        if term > self.term:
            self._follow(term)
        if kind == "vote":
            up_to_date = (message["last_term"], message["last_index"]) >= (
                self.log[-1].term, len(self.log) - 1)
            granted = (term == self.term and up_to_date and
                       self.voted_for in (None, source))
            if granted:
                self.voted_for = source
                self._persist()
                self._reset_timeout()
            self._send(source, "vote_reply", granted=granted)
        elif kind == "vote_reply":
            if term == self.term and self.role == "candidate" and message["granted"]:
                self.votes.add(source)
                if len(self.votes) >= 2:
                    self.role, self.leader = "leader", self.address
                    self.next_index = {peer: len(self.log) for peer in self.peers}
                    self.match_index = {peer: 0 for peer in self.peers}
                    # A current-term entry lets us commit inherited entries safely.
                    self.propose(None)
        elif kind == "append":
            self._append(source, message)
        elif kind == "append_reply" and term == self.term and self.role == "leader":
            if message["success"]:
                self.match_index[source] = max(self.match_index[source], message["match"])
                self.next_index[source] = self.match_index[source] + 1
                # Only entries from this term may advance the commit index.
                majority = sorted([len(self.log) - 1, *self.match_index.values()])[1]
                if majority > self.commit_index and self.log[majority].term == self.term:
                    self.commit_index = majority
                    self._apply()
                    self._broadcast()
                elif self.next_index[source] < len(self.log):
                    self._replicate(source)
            elif message["prev_index"] == self.next_index[source] - 1:
                self.next_index[source] = max(1, min(
                    self.next_index[source] - 1, message["match"] + 1))
                self.match_index[source] = min(
                    self.match_index[source], self.next_index[source] - 1)
                self._replicate(source)

    def _append(self, source, message):
        prev = message["prev_index"]
        if message["term"] < self.term:
            self._send(source, "append_reply", success=False, prev_index=prev,
                       match=len(self.log) - 1)
            return
        if self.role != "follower":
            self._follow(self.term)
        self.leader = source
        self._reset_timeout()
        if prev >= len(self.log) or self.log[prev].term != message["prev_term"]:
            self._send(source, "append_reply", success=False, prev_index=prev,
                       match=min(prev - 1, len(self.log) - 1))
            return
        entries = message["entries"]
        for index, entry in enumerate(entries, prev + 1):
            if index < len(self.log) and self.log[index].term != entry.term:
                # A valid leader never overwrites a committed entry.
                if index <= self.commit_index:
                    raise ValueError("attempt to overwrite committed log")
                del self.log[index:]
            if index == len(self.log):
                self.log.append(entry)
        if entries:
            self._persist()
        self.commit_index = max(self.commit_index, min(
            message["leader_commit"], prev + len(entries)))
        self._apply()
        self._send(source, "append_reply", success=True, prev_index=prev,
                   match=prev + len(entries))

    def propose(self, command):
        """Return (index, term); success is visible in results only after commit.

        Reads also enter the log, providing a quorum barrier against stale reads.
        A timeout means an unknown outcome, so clients must not assume failure.
        """
        if self.role != "leader":
            raise ValueError("not leader")
        self.log.append(Entry(self.term, command))
        self._persist()
        self._broadcast()
        return len(self.log) - 1, self.term

    def _apply(self):
        while self.applied < self.commit_index:
            self.applied += 1
            entry = self.log[self.applied]
            if entry.command is not None:
                self.results[self.applied] = self.apply(entry.command)

    def _replicate(self, peer):
        start = self.next_index[peer]
        self._send(peer, "append", prev_index=start - 1,
                   prev_term=self.log[start - 1].term,
                   entries=self.log[start:start + 32], leader_commit=self.commit_index)

    def _broadcast(self):
        self.next_heartbeat = self.clock() + self.heartbeat
        for peer in self.peers:
            self._replicate(peer)


class MemoryNetwork:
    """Deterministic queued transport with explicit time, loss and crash controls."""

    def __init__(self):
        self.now = 0.0
        self.nodes = {}
        self.queue = deque()
        self.blocked = set()
        self.drop = lambda source, target, message: False

    def clock(self):
        return self.now

    def send(self, source, target, message):
        if self._connected(source, target) and not self.drop(source, target, message):
            self.queue.append((source, target, message))

    def _connected(self, source, target):
        return source in self.nodes and target in self.nodes and (source, target) not in self.blocked

    def deliver(self):
        while self.queue:
            source, target, message = self.queue.popleft()
            if self._connected(source, target):
                self.nodes[target].receive(source, message)

    def advance(self, seconds, step=0.01):
        end = self.now + seconds
        while self.now < end:
            self.now = min(end, self.now + step)
            for node in list(self.nodes.values()):
                node.tick()
            self.deliver()

    def partition(self, *groups):
        self.blocked = {(source, target) for i, group in enumerate(groups)
                        for other in groups[i + 1:] for source in group for target in other}
        self.blocked |= {(target, source) for source, target in self.blocked}

    def heal(self):
        self.blocked.clear()

    def kill(self, address):
        self.nodes.pop(address)
        self.queue = deque((source, target, message) for source, target, message in self.queue
                           if address not in (source, target))

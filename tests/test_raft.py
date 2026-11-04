"""Drive elections, failures and replication using virtual time, never sleeps."""

from collections import deque

import pytest

from minikv.raft import MemoryNetwork, Raft
from minikv.resp import SimpleString
from minikv.server import execute
from minikv.store import Store


class Cluster:
    addresses = ("a", "b", "c")

    def __init__(self, seed=0, directory=None):
        self.network = MemoryNetwork()
        self.seed, self.directory = seed, directory
        self.stores = {}
        for address in self.addresses:
            self.start(address)

    def start(self, address):
        store = self.stores[address] = Store()
        node = Raft(address, [peer for peer in self.addresses if peer != address],
                    clock=self.network.clock, send=self.network.send,
                    apply=lambda request: execute(request, store),
                    seed=self.seed + self.addresses.index(address),
                    state_path=None if self.directory is None else self.directory / f"{address}.json")
        self.network.nodes[address] = node
        return node

    def leader(self, members=None):
        nodes = self.network.nodes
        leaders = [node for address, node in nodes.items()
                   if node.role == "leader" and (members is None or address in members)]
        assert len(leaders) == 1
        return leaders[0]

    def commit(self, *command, leader=None):
        node = leader or self.leader()
        index, _ = node.propose(list(command))
        self.network.advance(0.25)
        assert index in node.results
        return node.results[index]


@pytest.mark.parametrize("seed", range(8))
def test_election_and_replicated_commands(seed):
    cluster = Cluster(seed)
    net = cluster.network
    net.advance(2)
    leader = cluster.leader()
    term = leader.term
    net.advance(3)
    assert cluster.leader() is leader
    assert leader.term == term
    assert cluster.commit(b"SET", b"\x00\xff", b"\r\nvalue") == SimpleString("OK")
    assert cluster.commit(b"GETVERSIONED", b"\x00\xff") == [b"\r\nvalue", 0]
    assert cluster.commit(b"SETIFNEWER", b"\x00\xff", b"new", b"7") == 1
    assert cluster.commit(b"INVALIDATE", b"\x00\xff", b"7") == 1
    assert cluster.commit(b"SETIFNEWER", b"\x00\xff", b"old", b"6") == 0
    assert cluster.commit(b"SET", b"\x00\xff", b"last") == SimpleString("OK")
    for store in cluster.stores.values():
        assert store.get_versioned(b"\x00\xff") == [b"last", 8]


@pytest.mark.parametrize("seed", range(8))
def test_killed_leader_preserves_acknowledged_writes(seed):
    cluster = Cluster(seed)
    net = cluster.network
    net.advance(2)
    old = cluster.leader()
    # Crash immediately on commit, before followers receive leader_commit.
    index, _ = old.propose([b"SET", b"key", b"committed"])
    while index not in old.results:
        source, target, message = net.queue.popleft()
        net.nodes[target].receive(source, message)
    assert old.results[index] == SimpleString("OK")
    net.kill(old.address)
    net.advance(3)
    new = cluster.leader()
    assert new.term > old.term
    assert cluster.commit(b"GET", b"key") == b"committed"
    assert cluster.commit(b"SET", b"after", b"election") == SimpleString("OK")
    for address in net.nodes:
        assert cluster.stores[address].get(b"key") == b"committed"
        assert cluster.stores[address].get(b"after") == b"election"


@pytest.mark.parametrize("seed", range(8))
def test_minority_cannot_commit_and_conflicting_suffix_is_replaced(seed):
    cluster = Cluster(seed)
    net = cluster.network
    net.advance(2)
    old = cluster.leader()
    assert cluster.commit(b"SET", b"key", b"before") == SimpleString("OK")
    majority = set(cluster.addresses) - {old.address}
    net.partition({old.address}, majority)
    uncommitted, _ = old.propose([b"SET", b"minority-only", b"lost"])
    stale_read, _ = old.propose([b"GET", b"key"])
    net.advance(3)
    assert uncommitted not in old.results
    assert stale_read not in old.results
    assert cluster.stores[old.address].get(b"minority-only") is None
    new = cluster.leader(majority)
    assert cluster.commit(b"SET", b"key", b"majority", leader=new) == SimpleString("OK")
    assert cluster.commit(b"GET", b"key", leader=new) == b"majority"
    net.heal()
    net.advance(2)
    assert cluster.leader() is new
    assert cluster.commit(b"GET", b"minority-only") is None
    for store in cluster.stores.values():
        assert store.get(b"key") == b"majority"
        assert store.get(b"minority-only") is None


@pytest.mark.parametrize("persistent", [False, True])
def test_follower_catches_up_after_restart(tmp_path, persistent):
    cluster = Cluster(directory=tmp_path if persistent else None)
    net = cluster.network
    net.advance(2)
    leader = cluster.leader()
    follower = next(address for address in cluster.addresses if address != leader.address)
    assert cluster.commit(b"SETIFNEWER", b"key", b"before", b"4") == 1
    # Cross a replication batch boundary before losing the follower's memory.
    for number in range(35):
        assert cluster.commit(b"SET", b"prefix", str(number).encode()) == SimpleString("OK")
    net.kill(follower)
    for number in range(40):
        assert cluster.commit(b"SET", b"key", str(number).encode()) == SimpleString("OK")
    assert cluster.commit(b"INVALIDATE", b"marker", b"20") == 1
    cluster.start(follower)
    net.advance(2)
    assert cluster.stores[follower].get_versioned(b"key") == [b"39", 44]
    assert cluster.stores[follower].get_versioned(b"prefix") == [b"34", 34]
    # Rebuilt tombstones must reject stale fills when this node becomes leader.
    net.kill(leader.address)
    net.advance(3)
    assert cluster.commit(b"SETIFNEWER", b"marker", b"stale", b"19") == 0
    assert cluster.commit(b"GETVERSIONED", b"key") == [b"39", 44]


def test_persisted_votes_prevent_double_vote_and_full_cluster_restarts(tmp_path):
    cluster = Cluster(directory=tmp_path)
    net = cluster.network
    net.advance(2)
    assert cluster.commit(b"SET", b"key", b"survives") == SimpleString("OK")
    old = cluster.leader()
    # The old leader has already voted for itself in this term.
    old_term = old.term
    net.kill(old.address)
    restarted = cluster.start(old.address)
    competitor = next(address for address in cluster.addresses if address != old.address)
    restarted.receive(competitor, {"kind": "vote", "term": old_term,
                                  "last_term": old_term + 1, "last_index": 1000})
    _, _, reply = net.queue.pop()
    assert reply["granted"] is False
    for address in cluster.addresses:
        net.kill(address)
    for address in cluster.addresses:
        cluster.start(address)
    net.advance(3)
    assert cluster.leader().term > old_term
    assert cluster.commit(b"GET", b"key") == b"survives"


def test_drops_duplicates_and_reordering_do_not_reapply_commands():
    cluster = Cluster()
    net = cluster.network
    net.drop = lambda source, target, message: True
    net.advance(2)
    assert all(node.role != "leader" for node in net.nodes.values())
    net.drop = lambda source, target, message: False
    net.advance(3)
    leader = cluster.leader()
    leader.propose([b"SET", b"key", b"once"])
    # Deliver each initial AppendEntries twice and in reverse order.
    queued = list(net.queue)
    net.queue = deque(reversed(queued + queued))
    net.deliver()
    net.advance(0.5)
    assert cluster.commit(b"GETVERSIONED", b"key") == [b"once", 0]
    for store in cluster.stores.values():
        assert store.get_versioned(b"key") == [b"once", 0]


def test_stale_log_cannot_win_an_election():
    cluster = Cluster()
    net = cluster.network
    net.advance(2)
    leader = cluster.leader()
    laggard = next(address for address in cluster.addresses if address != leader.address)
    majority = set(cluster.addresses) - {laggard}
    net.partition({laggard}, majority)
    assert cluster.commit(b"SET", b"key", b"safe") == SimpleString("OK")
    net.advance(3)  # The isolated stale candidate has a higher term.
    net.heal()
    net.advance(4)
    assert cluster.commit(b"GET", b"key") == b"safe"
    assert all(store.get(b"key") == b"safe" for store in cluster.stores.values())

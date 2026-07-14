from __future__ import annotations

from threading import Lock
from typing import List


class RoundRobinDistributor:
    """Round-robin chunk-to-node assignment utilities."""

    def __init__(self, nodes: List[str]):
        if not nodes:
            raise ValueError("nodes must be a non-empty list")
        self.nodes = list(nodes)
        self._lock = Lock()
        self._next = 0

    # WHAT: Deterministically map a chunk index to a storage node using modulo.
    # WHY: This is O(1) time and O(1) space per assignment and is fully deterministic
    #      (same chunk index always maps to the same node). It avoids locks and
    #      internal state, making it ideal for retryable or idempotent orchestration.
    #      An alternative (least-loaded) requires tracking live load metrics and
    #      selecting the node with minimum load; that typically uses a heap or
    #      balanced tree with O(log N) selection and additional state synchronization
    #      under concurrent updates.
    # TRADE-OFF: Modulo-based round-robin ignores current node load and may
    #            produce uneven distribution if chunk sizes or concurrent uploads vary.
    def assign_by_index(self, chunk_index: int) -> str:
        if chunk_index < 0:
            raise ValueError("chunk_index must be non-negative")
        return self.nodes[chunk_index % len(self.nodes)]

    # WHAT: Assign the next node in a round-robin sequence using internal state.
    # WHY: Useful for streaming uploads where callers don't want to manage chunk
    #      indices. This uses a small lock to make increments thread-safe (O(1)).
    # TRADE-OFF: Keeps mutable state and requires locking for concurrency; for
    #            global determinism prefer `assign_by_index` instead.
    def assign_next(self) -> str:
        with self._lock:
            node = self.nodes[self._next]
            self._next = (self._next + 1) % len(self.nodes)
            return node

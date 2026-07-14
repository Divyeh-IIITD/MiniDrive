from __future__ import annotations

import urllib.request
from typing import List

from .distribution import RoundRobinDistributor
from .chunking import Chunk


class ReplicationError(Exception):
    pass


class Replicator:
    def __init__(self, node_urls: List[str], replication_factor: int = 2):
        if replication_factor < 1:
            raise ValueError("replication_factor must be >= 1")
        if replication_factor > len(node_urls):
            raise ValueError("replication_factor cannot exceed number of nodes")
        self.node_urls = list(node_urls)
        self.replication_factor = replication_factor
        self.distributor = RoundRobinDistributor(self.node_urls)

    # WHAT: Ensure a chunk is written to `replication_factor` distinct storage nodes.
    # WHY: Writing to multiple nodes increases durability; choosing nodes by
    #      chunk index via round-robin is O(1) to compute and deterministic so
    #      retries and orchestration can independently recompute targets. Each
    #      write is a network I/O operation; failures are handled by attempting
    #      alternate nodes until the replication factor is met.
    # TRADE-OFF: This sequential-write approach minimizes coordination complexity
    #            and memory usage (writes stream from bytes) but increases latency
    #            compared with parallel writes. Parallel writes would reduce
    #            wall-clock time but require more careful failure aggregation and
    #            more simultaneous network resources.
    def replicate_chunk(self, chunk: Chunk) -> List[str]:
        n = len(self.node_urls)
        primary_idx = chunk.index % n

        # deterministic ordering: start at primary_idx and walk the ring
        candidates = [self.node_urls[(primary_idx + i) % n] for i in range(n)]

        successes: List[str] = []
        errors: List[Exception] = []

        for node in candidates:
            if len(successes) >= self.replication_factor:
                break

            try:
                self._post_chunk_to_node(node, chunk)
                successes.append(node)
            except Exception as exc:  # network/IO errors
                errors.append(exc)
                # try next candidate until we reach replication_factor
                continue

        if len(successes) < self.replication_factor:
            raise ReplicationError(
                f"Failed to achieve replication={self.replication_factor}; "
                f"succeeded={len(successes)}; errors={errors}"
            )

        return successes

    def _post_chunk_to_node(self, node_url: str, chunk: Chunk) -> None:
        req = urllib.request.Request(node_url, data=chunk.data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            code = getattr(resp, "getcode", lambda: None)()
            if code is None:
                # Some file-like responses may not have getcode; assume success
                return
            if code >= 400:
                raise IOError(f"node {node_url} returned status {code}")

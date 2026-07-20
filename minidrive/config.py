from __future__ import annotations


# Static node URLs are intentional for this integration pass: they let us prove
# the coordinator/storage-node upload path end to end before introducing node
# discovery as a second moving part.
STORAGE_NODE_URLS = [
    "http://127.0.0.1:8001/chunks",
    "http://127.0.0.1:8002/chunks",
    "http://127.0.0.1:8003/chunks",
]

REPLICATION_FACTOR = 2
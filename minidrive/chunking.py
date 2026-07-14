from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO, Iterator


DEFAULT_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True, slots=True)
class Chunk:
    index: int
    data: bytes


# WHAT: Yield fixed-size byte chunks from a binary stream without reading it all at once.
# WHY: Repeated read(chunk_size) calls keep memory at O(chunk_size) instead of O(file_size), while preserving sequential I/O that works well for uploads and avoids extra copying compared with buffering the full file first.
# TRADE-OFF: This is single-pass streaming, so random access or retrying a later chunk requires rewinding or reopening the stream.
def iter_chunks(stream: BinaryIO, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Iterator[Chunk]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    index = 0
    while True:
        data = stream.read(chunk_size)
        if not data:
            break
        yield Chunk(index=index, data=data)
        index += 1

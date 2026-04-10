"""
Transform abstraction for mutating a file byte-stream before it reaches the target.

Add a new step (e.g. gzip compression, AES encryption) by subclassing Transform
and appending an instance to the TransformPipeline passed to sync.transfer().
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator, List


class Transform(ABC):
    """Stateless, streaming transformation applied to a file's byte-stream."""

    @abstractmethod
    def apply(self, chunks: Iterator[bytes]) -> Iterator[bytes]:
        """
        Consume *chunks* and yield transformed bytes.
        The implementation may change the number / size of output chunks.
        """


class TransformPipeline:
    """Chains zero or more Transforms together in sequence."""

    def __init__(self, transforms: List[Transform] | None = None) -> None:
        self.transforms: List[Transform] = transforms or []

    def apply(self, chunks: Iterator[bytes]) -> Iterator[bytes]:
        stream = chunks
        for transform in self.transforms:
            stream = transform.apply(stream)
        return stream

    def __len__(self) -> int:
        return len(self.transforms)

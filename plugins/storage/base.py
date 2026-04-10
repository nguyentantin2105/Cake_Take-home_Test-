"""Abstract storage backend — swap SFTP for S3/GCS by implementing this interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, List


@dataclass(frozen=True)
class FileInfo:
    """Metadata for a single file on a storage backend."""
    path: str           # path relative to the backend's root
    size: int           # bytes
    modified_at: datetime


class StorageBackend(ABC):
    """
    Read/write abstraction for any file storage system.

    Implementations exist for SFTP today; adding S3 or GCS requires only a
    new subclass — the DAG and sync logic are unchanged.
    """

    # ------------------------------------------------------------------
    # Required interface
    # ------------------------------------------------------------------

    @abstractmethod
    def list_files(self, prefix: str = "") -> List[FileInfo]:
        """Recursively list every file under *prefix* (relative to root)."""

    @abstractmethod
    def read_chunks(self, path: str, chunk_size: int = 8 * 1024 * 1024) -> Iterator[bytes]:
        """
        Yield *chunk_size*-byte chunks of *path* without loading it fully
        into memory.  Callers must exhaust or close the iterator.
        """

    @abstractmethod
    def write_chunks(self, path: str, chunks: Iterator[bytes]) -> None:
        """
        Write a file from a stream of chunks, creating parent directories
        as needed.  Overwrites any existing file at *path*.
        """

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Return True if *path* refers to an existing file."""

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Release underlying connections / resources."""

    def __enter__(self) -> "StorageBackend":
        return self

    def __exit__(self, *_) -> None:
        self.close()

"""SFTP implementation of StorageBackend using paramiko."""
from __future__ import annotations

import os
import stat
from datetime import datetime
from typing import Iterator, List, Optional

import paramiko

from storage.base import FileInfo, StorageBackend

_DEFAULT_CHUNK = 8 * 1024 * 1024  # 8 MB


class SFTPBackend(StorageBackend):
    """
    StorageBackend backed by an SFTP server.

    All paths exposed through the public API are *relative* to *root_path*.
    The underlying SFTP transport is kept open for the lifetime of the object;
    use it as a context manager to ensure clean teardown.
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: Optional[str] = None,
        key_path: Optional[str] = None,
        root_path: str = "/",
    ) -> None:
        self._root = root_path.rstrip("/")
        self._transport = paramiko.Transport((host, int(port)))
        if key_path:
            pkey = paramiko.RSAKey.from_private_key_file(key_path)
            self._transport.connect(username=username, pkey=pkey)
        else:
            self._transport.connect(username=username, password=password)
        self._sftp = paramiko.SFTPClient.from_transport(self._transport)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _abs(self, rel: str) -> str:
        """Convert a relative path to an absolute path on the server."""
        return f"{self._root}/{rel.lstrip('/')}" if rel else self._root

    def _walk(self, abs_dir: str, rel_dir: str, result: List[FileInfo]) -> None:
        try:
            entries = self._sftp.listdir_attr(abs_dir)
        except IOError:
            return
        for entry in entries:
            rel_path = f"{rel_dir}/{entry.filename}".lstrip("/")
            abs_path = f"{abs_dir}/{entry.filename}"
            if stat.S_ISDIR(entry.st_mode):
                self._walk(abs_path, rel_path, result)
            else:
                result.append(
                    FileInfo(
                        path=rel_path,
                        size=entry.st_size,
                        modified_at=datetime.fromtimestamp(entry.st_mtime),
                    )
                )

    def _makedirs(self, abs_dir: str) -> None:
        """Recursively create directories, ignoring existing ones."""
        parts = abs_dir.lstrip("/").split("/")
        current = ""
        for part in parts:
            current = f"{current}/{part}"
            try:
                self._sftp.stat(current)
            except IOError:
                self._sftp.mkdir(current)

    # ------------------------------------------------------------------
    # StorageBackend interface
    # ------------------------------------------------------------------

    def list_files(self, prefix: str = "") -> List[FileInfo]:
        files: List[FileInfo] = []
        self._walk(self._abs(prefix), prefix, files)
        return files

    def read_chunks(self, path: str, chunk_size: int = _DEFAULT_CHUNK) -> Iterator[bytes]:
        with self._sftp.open(self._abs(path), "rb") as fh:
            fh.prefetch()  # ask paramiko to pipeline reads
            while True:
                chunk = fh.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def write_chunks(self, path: str, chunks: Iterator[bytes]) -> None:
        abs_path = self._abs(path)
        self._makedirs(os.path.dirname(abs_path))
        with self._sftp.open(abs_path, "wb") as fh:
            for chunk in chunks:
                fh.write(chunk)

    def exists(self, path: str) -> bool:
        try:
            self._sftp.stat(self._abs(path))
            return True
        except IOError:
            return False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        try:
            self._sftp.close()
        finally:
            self._transport.close()

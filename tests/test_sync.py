"""Tests for the pure business-logic layer (sync.find_new_files, sync.transfer)."""
import os
import sys
import unittest
from datetime import datetime
from typing import Iterator, List

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PLUGINS_DIR = os.path.join(PROJECT_ROOT, "plugins")
if PLUGINS_DIR not in sys.path:
    sys.path.insert(0, PLUGINS_DIR)

from storage.base import FileInfo, StorageBackend
from transforms.base import Transform, TransformPipeline
from sync import find_new_files, transfer


# ------------------------------------------------------------------
# In-memory backend for testing (no network, no paramiko)
# ------------------------------------------------------------------

class MemoryBackend(StorageBackend):
    """Fully in-memory StorageBackend for unit tests."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self._files: dict[str, bytes] = dict(files or {})

    def list_files(self, prefix: str = "") -> List[FileInfo]:
        return [
            FileInfo(path=p, size=len(data), modified_at=datetime(2024, 1, 1))
            for p, data in sorted(self._files.items())
            if p.startswith(prefix)
        ]

    def read_chunks(self, path: str, chunk_size: int = 8 * 1024 * 1024) -> Iterator[bytes]:
        data = self._files[path]
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    def write_chunks(self, path: str, chunks: Iterator[bytes]) -> None:
        self._files[path] = b"".join(chunks)

    def exists(self, path: str) -> bool:
        return path in self._files


# ------------------------------------------------------------------
# Tests for find_new_files
# ------------------------------------------------------------------

class TestFindNewFiles(unittest.TestCase):
    def test_returns_all_when_none_transferred(self):
        source = MemoryBackend({
            "a/file1.txt": b"hello",
            "b/file2.txt": b"world",
        })
        result = find_new_files(source, is_transferred=lambda _: False)
        self.assertEqual(len(result), 2)

    def test_excludes_already_transferred(self):
        source = MemoryBackend({
            "a/file1.txt": b"hello",
            "b/file2.txt": b"world",
        })
        done = {"a/file1.txt"}
        result = find_new_files(source, is_transferred=lambda f: f.path in done)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].path, "b/file2.txt")

    def test_returns_empty_when_all_transferred(self):
        source = MemoryBackend({"x.txt": b"data"})
        result = find_new_files(source, is_transferred=lambda _: True)
        self.assertEqual(result, [])

    def test_prefix_filter(self):
        source = MemoryBackend({
            "a/file1.txt": b"1",
            "b/file2.txt": b"2",
        })
        result = find_new_files(source, is_transferred=lambda _: False, prefix="a/")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].path, "a/file1.txt")

    def test_empty_source(self):
        source = MemoryBackend()
        result = find_new_files(source, is_transferred=lambda _: False)
        self.assertEqual(result, [])


# ------------------------------------------------------------------
# Tests for transfer
# ------------------------------------------------------------------

class TestTransfer(unittest.TestCase):
    def test_transfer_copies_content(self):
        source = MemoryBackend({"doc.txt": b"hello world"})
        target = MemoryBackend()
        info = source.list_files()[0]

        transfer(info, source, target)

        self.assertTrue(target.exists("doc.txt"))
        self.assertEqual(target._files["doc.txt"], b"hello world")

    def test_transfer_preserves_path(self):
        source = MemoryBackend({"a/b/c/deep.txt": b"deep content"})
        target = MemoryBackend()
        info = source.list_files()[0]

        transfer(info, source, target)

        self.assertIn("a/b/c/deep.txt", target._files)

    def test_transfer_with_transform(self):
        """Transform that uppercases ASCII bytes."""

        class UpperTransform(Transform):
            def apply(self, chunks: Iterator[bytes]) -> Iterator[bytes]:
                for chunk in chunks:
                    yield chunk.upper()

        source = MemoryBackend({"file.txt": b"hello"})
        target = MemoryBackend()
        info = source.list_files()[0]
        pipeline = TransformPipeline([UpperTransform()])

        transfer(info, source, target, pipeline)

        self.assertEqual(target._files["file.txt"], b"HELLO")

    def test_transfer_large_file_chunked(self):
        """Ensure multi-chunk files are reassembled correctly."""
        data = b"x" * 100
        source = MemoryBackend({"big.bin": data})
        target = MemoryBackend()
        info = source.list_files()[0]

        # Force small chunk size via read_chunks
        original_read = source.read_chunks
        source.read_chunks = lambda path, chunk_size=10: original_read(path, chunk_size=10)

        transfer(info, source, target)

        self.assertEqual(target._files["big.bin"], data)

    def test_transfer_empty_file(self):
        source = MemoryBackend({"empty.txt": b""})
        target = MemoryBackend()
        info = source.list_files()[0]

        transfer(info, source, target)

        self.assertTrue(target.exists("empty.txt"))
        self.assertEqual(target._files["empty.txt"], b"")


if __name__ == "__main__":
    unittest.main()

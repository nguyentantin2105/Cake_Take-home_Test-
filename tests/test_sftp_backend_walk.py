import os
import stat
import sys
import unittest
from dataclasses import dataclass
from types import SimpleNamespace


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PLUGINS_DIR = os.path.join(PROJECT_ROOT, "plugins")
if PLUGINS_DIR not in sys.path:
    sys.path.insert(0, PLUGINS_DIR)

# Allow importing SFTPBackend in environments without paramiko installed.
sys.modules.setdefault(
    "paramiko",
    SimpleNamespace(
        Transport=object,
        RSAKey=SimpleNamespace(from_private_key_file=lambda *_args, **_kwargs: None),
        SFTPClient=SimpleNamespace(from_transport=lambda *_args, **_kwargs: None),
    ),
)

from storage.sftp_backend import SFTPBackend


@dataclass
class _FakeEntry:
    filename: str
    st_mode: int
    st_size: int = 0
    st_mtime: int = 0


class _FakeSFTP:
    def __init__(self, listing_by_path):
        self.listing_by_path = listing_by_path

    def listdir_attr(self, path):
        value = self.listing_by_path.get(path)
        if isinstance(value, Exception):
            raise value
        if value is None:
            raise IOError(f"Path not found: {path}")
        return value


class TestSFTPBackendWalk(unittest.TestCase):
    def _build_backend(self, listing_by_path):
        backend = SFTPBackend.__new__(SFTPBackend)
        backend._sftp = _FakeSFTP(listing_by_path)  # noqa: SLF001 - testing private internals
        return backend

    def test_walk_recurses_and_collects_only_files(self):
        listing = {
            "/root": [
                _FakeEntry("dir_a", stat.S_IFDIR),
                _FakeEntry("file_root.txt", stat.S_IFREG, st_size=10, st_mtime=1700000000),
            ],
            "/root/dir_a": [
                _FakeEntry("nested.txt", stat.S_IFREG, st_size=20, st_mtime=1700000001),
                _FakeEntry("dir_b", stat.S_IFDIR),
            ],
            "/root/dir_a/dir_b": [
                _FakeEntry("deep.txt", stat.S_IFREG, st_size=30, st_mtime=1700000002),
            ],
        }
        backend = self._build_backend(listing)

        result = []
        backend._walk("/root", "", result)  # noqa: SLF001 - testing private internals

        paths = sorted(f.path for f in result)
        self.assertEqual(paths, ["dir_a/dir_b/deep.txt", "dir_a/nested.txt", "file_root.txt"])
        self.assertEqual(sum(f.size for f in result), 60)

    def test_walk_ignores_missing_or_unreadable_directory(self):
        backend = self._build_backend(
            {
                "/root": IOError("permission denied"),
            }
        )

        result = []
        backend._walk("/root", "", result)  # noqa: SLF001 - testing private internals

        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()

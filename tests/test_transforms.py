"""Tests for the transform pipeline."""
import os
import sys
import unittest
from typing import Iterator

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PLUGINS_DIR = os.path.join(PROJECT_ROOT, "plugins")
if PLUGINS_DIR not in sys.path:
    sys.path.insert(0, PLUGINS_DIR)

from transforms.base import Transform, TransformPipeline


class _DoubleTransform(Transform):
    """Test transform that repeats every chunk twice."""

    def apply(self, chunks: Iterator[bytes]) -> Iterator[bytes]:
        for chunk in chunks:
            yield chunk + chunk


class _PrefixTransform(Transform):
    """Test transform that prepends a header to the first chunk."""

    def __init__(self, prefix: bytes) -> None:
        self._prefix = prefix

    def apply(self, chunks: Iterator[bytes]) -> Iterator[bytes]:
        first = True
        for chunk in chunks:
            if first:
                yield self._prefix + chunk
                first = False
            else:
                yield chunk


class TestTransformPipeline(unittest.TestCase):
    def test_empty_pipeline_passes_through(self):
        pipeline = TransformPipeline()
        chunks = [b"hello", b" world"]
        result = list(pipeline.apply(iter(chunks)))
        self.assertEqual(result, [b"hello", b" world"])

    def test_single_transform(self):
        pipeline = TransformPipeline([_DoubleTransform()])
        result = list(pipeline.apply(iter([b"ab"])))
        self.assertEqual(result, [b"abab"])

    def test_chained_transforms(self):
        pipeline = TransformPipeline([
            _PrefixTransform(b"["),
            _DoubleTransform(),
        ])
        result = list(pipeline.apply(iter([b"hi"])))
        # PrefixTransform: [b"[hi"] -> DoubleTransform: [b"[hi[hi"]
        self.assertEqual(result, [b"[hi[hi"])

    def test_len(self):
        self.assertEqual(len(TransformPipeline()), 0)
        self.assertEqual(len(TransformPipeline([_DoubleTransform()])), 1)
        self.assertEqual(len(TransformPipeline([_DoubleTransform(), _PrefixTransform(b"x")])), 2)

    def test_empty_input(self):
        pipeline = TransformPipeline([_DoubleTransform()])
        result = list(pipeline.apply(iter([])))
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()

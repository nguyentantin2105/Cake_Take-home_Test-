"""
Pure business logic for the SFTP sync workflow.

No Airflow imports — this module is independently testable and reusable
outside of an Airflow context.
"""
from __future__ import annotations

import logging
from typing import Callable, List

from storage.base import FileInfo, StorageBackend
from transforms.base import TransformPipeline

log = logging.getLogger(__name__)


def find_new_files(
    source: StorageBackend,
    is_transferred: Callable[[FileInfo], bool],
    prefix: str = "",
) -> List[FileInfo]:
    """
    Return files present on *source* that have not yet been transferred.

    Args:
        source:         Backend to list files from.
        is_transferred: Predicate returning True when a file was already
                        copied to the target.  Injected by the caller so
                        this function stays free of any state-store dependency.
        prefix:         Optional sub-path to restrict the listing.
    """
    all_files = source.list_files(prefix=prefix)
    new_files = [f for f in all_files if not is_transferred(f)]
    log.info(
        "Source has %d file(s); %d new/pending transfer(s).",
        len(all_files),
        len(new_files),
    )
    return new_files


def transfer(
    file_info: FileInfo,
    source: StorageBackend,
    target: StorageBackend,
    pipeline: TransformPipeline | None = None,
) -> None:
    """
    Stream a single file from *source* through *pipeline* into *target*.

    The file is never fully buffered in memory: data flows chunk-by-chunk,
    making this safe for files of arbitrary size.

    Args:
        file_info: Metadata describing the file to transfer.
        source:    Backend to read from.
        target:    Backend to write to.
        pipeline:  Optional chain of in-flight transformations.
                   Defaults to a no-op pipeline.
    """
    if pipeline is None:
        pipeline = TransformPipeline()

    log.info("Transferring '%s' (%d bytes).", file_info.path, file_info.size)
    chunks = source.read_chunks(file_info.path)
    target.write_chunks(file_info.path, pipeline.apply(chunks))
    log.info("Transfer complete: '%s'.", file_info.path)

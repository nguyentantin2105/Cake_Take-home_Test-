"""
sftp_sync DAG
=============
Periodically scans the source SFTP server for files not yet present on the
target SFTP server and copies them, preserving the full directory hierarchy.

Key design choices
------------------
* Unidirectional, append-only: files deleted on source are never removed from
  target; files already transferred (tracked via Airflow Variables) are skipped.
* Chunked streaming: files are never fully loaded into RAM, so gigabyte-sized
  files are handled as safely as kilobyte-sized ones.
* Dynamic task mapping: each file to be transferred becomes an independent task
  instance, which Airflow can distribute across workers and retry individually.
* Pluggable transforms: a TransformPipeline can be configured below to add
  compression, encryption, or any other byte-level mutation before writing.
* Swappable backends: changing the data source from SFTP to S3 requires only
  replacing SFTPBackend with an S3Backend that implements StorageBackend.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from typing import List

# Airflow prepends /opt/airflow/plugins to sys.path, but add explicitly for
# local development / testing outside the container.
if "/opt/airflow/plugins" not in sys.path:
    sys.path.insert(0, "/opt/airflow/plugins")

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from storage.base import FileInfo
from storage.sftp_backend import SFTPBackend
from sync import find_new_files, transfer
from transforms.base import TransformPipeline

log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

MANIFEST_VAR_PREFIX = "sftp_sync_done"   # Airflow Variable key prefix
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024     # 8 MB


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _make_backend(conn_id: str, root_path: str | None = None) -> SFTPBackend:
    """
    Build an SFTPBackend from an Airflow connection.

    The optional *root_path* overrides the one stored in the connection's
    Extra JSON field (key ``root_path``).
    """
    conn = BaseHook.get_connection(conn_id)
    extra = json.loads(conn.extra) if conn.extra else {}
    resolved_root = root_path or extra.get("root_path", "/")
    return SFTPBackend(
        host=conn.host,
        port=conn.port or 22,
        username=conn.login,
        password=conn.get_password(),
        key_path=extra.get("key_path"),
        root_path=resolved_root,
    )


def _manifest_key(conn_id: str) -> str:
    return f"{MANIFEST_VAR_PREFIX}__{conn_id}"


def _load_manifest(conn_id: str) -> set[str]:
    raw = Variable.get(_manifest_key(conn_id), default_var="[]")
    return set(json.loads(raw))


def _save_manifest(conn_id: str, manifest: set[str]) -> None:
    Variable.set(_manifest_key(conn_id), json.dumps(sorted(manifest)))


# ------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------

@dag(
    dag_id="sftp_sync",
    description="Incrementally copy new files from source SFTP to target SFTP.",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    params={
        "source_conn_id": "sftp_source",
        "target_conn_id": "sftp_target",
        # Override connection-level root paths when needed:
        "source_root": None,
        "target_root": None,
        # Restrict sync to a subdirectory (empty = entire root):
        "source_prefix": "",
    },
    tags=["sftp", "sync"],
    doc_md=__doc__,
)
def sftp_sync() -> None:

    @task
    def list_pending_files(**context) -> List[dict]:
        """
        Connect to source, list all files, and return those not yet recorded
        in the transfer manifest.  Returns a list of serialisable dicts so
        Airflow can pass them through XCom to the downstream mapped tasks.
        """
        params = context["params"]
        source_conn_id: str = params["source_conn_id"]

        manifest = _load_manifest(source_conn_id)

        def already_done(info: FileInfo) -> bool:
            return info.path in manifest

        with _make_backend(source_conn_id, params.get("source_root")) as source:
            pending = find_new_files(
                source,
                is_transferred=already_done,
                prefix=params.get("source_prefix", ""),
            )

        return [
            {
                "path": f.path,
                "size": f.size,
                "modified_at": f.modified_at.isoformat(),
            }
            for f in pending
        ]

    @task(max_active_tis_per_dagrun=4)
    def copy_file(file_dict: dict, **context) -> str:
        """
        Transfer a single file from source to target.

        Runs as an independent task instance — retried automatically by
        Airflow on failure without re-running the full DAG.

        Returns the transferred file path so the downstream
        ``update_manifest`` task can record it in a single, race-free write.
        """
        params = context["params"]
        source_conn_id: str = params["source_conn_id"]
        target_conn_id: str = params["target_conn_id"]

        file_info = FileInfo(
            path=file_dict["path"],
            size=file_dict["size"],
            modified_at=datetime.fromisoformat(file_dict["modified_at"]),
        )

        # Extend this pipeline to add compression, encryption, etc.
        pipeline = TransformPipeline()

        with (
            _make_backend(source_conn_id, params.get("source_root")) as source,
            _make_backend(target_conn_id, params.get("target_root")) as target,
        ):
            transfer(file_info, source, target, pipeline)

        log.info("Transfer complete, path will be recorded by update_manifest.")
        return file_info.path

    @task(trigger_rule="all_done")
    def update_manifest(transferred_paths: list, **context) -> None:
        """
        Merge all successfully transferred paths into the manifest in a
        single write — eliminates the race condition of concurrent tasks
        each doing load-modify-save independently.
        """
        params = context["params"]
        source_conn_id: str = params["source_conn_id"]

        # Filter out None values from failed/skipped upstream tasks.
        new_paths = [p for p in (transferred_paths or []) if p]
        if not new_paths:
            log.info("No new paths to record.")
            return

        manifest = _load_manifest(source_conn_id)
        manifest.update(new_paths)
        _save_manifest(source_conn_id, manifest)
        log.info("Manifest updated with %d path(s).", len(new_paths))

    pending = list_pending_files()
    copied = copy_file.expand(file_dict=pending)
    update_manifest(copied)


sftp_sync()

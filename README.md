# SFTP Sync — Airflow DAG

Incrementally copies files from a **source** SFTP server to a **target** SFTP server using Apache Airflow with the Celery executor.

---

## Architecture

```
plugins/
  storage/
    base.py           — StorageBackend ABC (swap SFTP → S3 here)
    sftp_backend.py   — Paramiko-based SFTP implementation
  transforms/
    base.py           — Transform ABC + TransformPipeline
  sync.py             — Pure business logic (no Airflow dependency)
dags/
  sftp_sync_dag.py    — Airflow DAG wiring everything together
```

The DAG runs every 5 minutes. On each run:

1. `**list_pending_files**` — connects to the source SFTP, lists all files recursively, and excludes paths already recorded in the Airflow Variable manifest.
2. `**copy_file**` *(dynamically mapped)* — one task instance per new file; streams the file in 8 MB chunks through an optional transform pipeline into the target SFTP. Returns the transferred path via XCom.
3. `**update_manifest*`* — collects all transferred paths from the mapped tasks and writes them to the manifest in a single, race-free operation.

---

## Prerequisites

- Docker >= 24 and Docker Compose v2
- Ports **18080** (Airflow UI), **2222** (source SFTP), **2223** (target SFTP) free on the host

---

## Quick start

```bash
# 1. Clone and enter the project
git clone https://github.com/nguyentantin2105/Cake_Take-home_Test.git
cd Cake_Take-home_Test

# 2. Create .env from the example and set your host UID
cp .env.example .env
sed -i'' -e "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env

# 3. Start all services (first run takes a few minutes to pull images)
docker compose up -d

# 4. Wait for initialisation to complete
docker compose logs -f airflow-init
# Look for: "Airflow initialisation complete."

# 5. Open the Airflow UI
open http://localhost:18080   # login: admin / admin

# 6. Unpause and trigger the DAG
#    UI → DAGs → sftp_sync → toggle ON → ▶ Trigger DAG
```

---

## Seeding the source SFTP with test data

```bash
# Copy a file into the source server's upload directory
docker compose exec sftp-source mkdir -p /home/user/upload/a/b/c
docker compose exec sftp-source bash -c \
  "echo 'hello world' > /home/user/upload/a/b/c/file_1.txt"
```

Trigger the DAG and verify the file appears on the target:

```bash
docker compose exec sftp-target cat /home/user/upload/a/b/c/file_1.txt
```

---

## Connecting to an external SFTP server

Edit the SFTP variables in `.env`:

```bash
# Source SFTP
SFTP_SOURCE_HOST=your.sftp.host
SFTP_SOURCE_USER=your_user
SFTP_SOURCE_PASSWORD=your_password
SFTP_SOURCE_PORT=22
SFTP_SOURCE_ROOT=/data

# Target SFTP
SFTP_TARGET_HOST=your.target.host
SFTP_TARGET_USER=your_user
SFTP_TARGET_PASSWORD=your_password
SFTP_TARGET_PORT=22
SFTP_TARGET_ROOT=/data
```

Then restart the init container to re-create the connections:

```bash
docker compose up -d --force-recreate airflow-init
```

Alternatively, update connections via the UI (**Admin → Connections**) or pass `source_conn_id`, `target_conn_id`, `source_root`, `target_root`, and `source_prefix` as run-time parameters (Trigger DAG → Conf).

---

## Resetting the transfer manifest

The list of already-transferred files is stored in Airflow Variables.  To force a full re-sync:

```bash
# From the Airflow UI: Admin → Variables → delete sftp_sync_done__sftp_source
# Or via CLI inside any container:
docker compose exec airflow-scheduler \
  airflow variables delete sftp_sync_done__sftp_source
```

---

## Running tests

```bash
# Unit tests (no Airflow or Docker required)
python -m unittest discover -s tests -v

# DAG integrity tests (run inside the scheduler container)
docker compose exec airflow-scheduler \
  python -m unittest tests/test_dag_integrity.py -v
```

---

## Assumptions and design decisions


| Topic                   | Decision                                                                     | Rationale                                                                                         |
| ----------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| **Sync direction**      | Source → target only; deletes on source are not propagated                   | Requirement: deleted source files must be preserved on target                                     |
| **Change detection**    | File path used as the deduplication key                                      | Simple and predictable; re-running after a manifest reset transfers everything again              |
| **Large files**         | Paramiko `prefetch` + 8 MB chunk streaming                                   | Files are never fully loaded into RAM; the chunk size is configurable per call                    |
| **Concurrency**         | `max_active_tis_per_dagrun=4`, `max_active_runs=1`                           | Limits SFTP connection pressure; prevents overlapping DAG runs from double-listing                |
| **Manifest update**     | A dedicated `update_manifest` task merges all paths in one write             | Eliminates the race condition of concurrent tasks each doing load-modify-save independently       |
| **Database**            | PostgreSQL instead of SQLite                                                 | SQLite does not support concurrent writes required by the Celery result backend                   |
| **Transform pipeline**  | `TransformPipeline` chains `Transform` objects over the byte stream          | Adding gzip/encryption requires a new `Transform` subclass — the DAG and sync logic are unchanged |
| **Backend abstraction** | `StorageBackend` ABC with `list_files / read_chunks / write_chunks / exists` | Replacing SFTP with S3 requires only a new class; the DAG calls the same interface                |


---

## Extending the solution

### Add a transform (e.g. gzip compression)

```python
# plugins/transforms/gzip_transform.py
import zlib
from typing import Iterator
from transforms.base import Transform

class GzipCompress(Transform):
    def apply(self, chunks: Iterator[bytes]) -> Iterator[bytes]:
        compressor = zlib.compressobj(wbits=31)  # gzip format
        for chunk in chunks:
            compressed = compressor.compress(chunk)
            if compressed:
                yield compressed
        yield compressor.flush()
```

Then in `sftp_sync_dag.py`:

```python
from transforms.gzip_transform import GzipCompress
pipeline = TransformPipeline([GzipCompress()])
```

### Swap SFTP for S3

```python
# plugins/storage/s3_backend.py
import boto3
from storage.base import StorageBackend, FileInfo

class S3Backend(StorageBackend):
    def __init__(self, bucket: str, prefix: str = "", **boto_kwargs):
        self._s3 = boto3.client("s3", **boto_kwargs)
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")

    def list_files(self, prefix=""):
        paginator = self._s3.get_paginator("list_objects_v2")
        ...  # implement using paginator

    def read_chunks(self, path, chunk_size=8*1024*1024):
        obj = self._s3.get_object(Bucket=self._bucket, Key=f"{self._prefix}/{path}")
        for chunk in obj["Body"].iter_chunks(chunk_size):
            yield chunk

    def write_chunks(self, path, chunks):
        # Use multipart upload for large files
        ...
```

Update `_make_backend()` in the DAG to return `S3Backend` when the connection type is `s3`.
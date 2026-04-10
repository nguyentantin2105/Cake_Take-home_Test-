"""
Smoke test — verify the DAG file is parseable and has expected structure.

Requires apache-airflow to be installed.  The tests are automatically skipped
when running outside the Airflow container (e.g. local dev without Airflow).
"""
import os
import sys
import unittest
from unittest.mock import patch
from types import SimpleNamespace

try:
    import airflow  # noqa: F401
    _HAS_AIRFLOW = True
except ImportError:
    _HAS_AIRFLOW = False

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PLUGINS_DIR = os.path.join(PROJECT_ROOT, "plugins")
if PLUGINS_DIR not in sys.path:
    sys.path.insert(0, PLUGINS_DIR)

# Paramiko is not installed in the test environment — stub it out.
sys.modules.setdefault(
    "paramiko",
    SimpleNamespace(
        Transport=object,
        RSAKey=SimpleNamespace(from_private_key_file=lambda *a, **kw: None),
        SFTPClient=SimpleNamespace(from_transport=lambda *a, **kw: None),
    ),
)


@unittest.skipUnless(_HAS_AIRFLOW, "apache-airflow not installed")
class TestDAGIntegrity(unittest.TestCase):
    """Ensure the DAG file can be parsed by Airflow without import errors."""

    @patch.dict(os.environ, {"AIRFLOW_HOME": "/tmp/airflow_test"})
    def test_dag_loads_without_errors(self):
        from airflow.models import DagBag

        dag_folder = os.path.join(PROJECT_ROOT, "dags")
        dagbag = DagBag(dag_folder=dag_folder, include_examples=False)

        self.assertEqual(
            dagbag.import_errors,
            {},
            f"DAG import errors: {dagbag.import_errors}",
        )

    @patch.dict(os.environ, {"AIRFLOW_HOME": "/tmp/airflow_test"})
    def test_dag_has_expected_tasks(self):
        from airflow.models import DagBag

        dag_folder = os.path.join(PROJECT_ROOT, "dags")
        dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
        dag = dagbag.get_dag("sftp_sync")

        self.assertIsNotNone(dag, "sftp_sync DAG not found")
        task_ids = {t.task_id for t in dag.tasks}
        self.assertIn("list_pending_files", task_ids)
        self.assertIn("copy_file", task_ids)
        self.assertIn("update_manifest", task_ids)

    @patch.dict(os.environ, {"AIRFLOW_HOME": "/tmp/airflow_test"})
    def test_dag_has_correct_schedule(self):
        from airflow.models import DagBag

        dag_folder = os.path.join(PROJECT_ROOT, "dags")
        dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
        dag = dagbag.get_dag("sftp_sync")

        self.assertIsNotNone(dag)
        # schedule is timedelta(minutes=5)
        from datetime import timedelta
        self.assertEqual(dag.schedule_interval, timedelta(minutes=5))


if __name__ == "__main__":
    unittest.main()

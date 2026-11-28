"""DAG import and structure tests. Run inside the Airflow container:
    docker compose run --rm airflow-cli python -m pytest tests/test_dag.py
"""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR))


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.get_dag("github_ingestion")


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_tasks(dag):
    assert set(dag.task_ids) == {
        "start_run", "check_api_quota", "extract_repositories", "validate_repositories",
        "load_and_quarantine", "finalize_run",
    }


@pytest.mark.parametrize("upstream, downstream", [
    ("start_run", "check_api_quota"),
    ("check_api_quota", "extract_repositories"),
    ("extract_repositories", "validate_repositories"),
    ("validate_repositories", "load_and_quarantine"),
    ("load_and_quarantine", "finalize_run"),
])
def test_dependencies(dag, upstream, downstream):
    assert downstream in dag.get_task(upstream).downstream_task_ids


def test_finalize_runs_on_failure_too(dag):
    assert dag.get_task("finalize_run").trigger_rule == "all_done"


def test_extract_and_validate_are_mapped_per_org(dag):
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    for task_id in ("extract_repositories", "validate_repositories"):
        assert isinstance(dag.get_task(task_id), MappedOperator)

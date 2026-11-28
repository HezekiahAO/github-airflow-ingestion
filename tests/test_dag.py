"""DAG import and structure tests. Run inside the Airflow container:
    docker compose run --rm airflow-cli python -m pytest tests/test_dag.py
"""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
COMMON_EDGES = [
    ("start_run", "plan_extraction"),
    ("plan_extraction", "extract_repositories"),
    ("extract_repositories", "validate_repositories"),
    ("validate_repositories", "load_and_quarantine"),
    ("load_and_quarantine", "finalize_run"),
]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR))


@pytest.fixture(scope="module")
def live(dagbag):
    return dagbag.get_dag("github_ingestion")


@pytest.fixture(scope="module")
def fixture_dag(dagbag):
    return dagbag.get_dag("github_ingestion_fixture")


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert {"github_ingestion", "github_ingestion_fixture"} <= set(dagbag.dag_ids)


@pytest.mark.parametrize("upstream, downstream", COMMON_EDGES + [
    ("plan_extraction", "check_api_quota"), ("check_api_quota", "extract_repositories")])
def test_live_dependencies(live, upstream, downstream):
    assert downstream in live.get_task(upstream).downstream_task_ids


@pytest.mark.parametrize("upstream, downstream", COMMON_EDGES)
def test_fixture_dependencies(fixture_dag, upstream, downstream):
    assert downstream in fixture_dag.get_task(upstream).downstream_task_ids


def test_finalize_runs_on_failure_too(live, fixture_dag):
    for d in (live, fixture_dag):
        assert d.get_task("finalize_run").trigger_rule == "all_done"


def test_extract_and_validate_are_mapped_per_org(live):
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    for task_id in ("extract_repositories", "validate_repositories"):
        assert isinstance(live.get_task(task_id), MappedOperator)


def test_schedule(live, fixture_dag):
    for d in (live, fixture_dag):
        assert d.timetable.summary == "0 0 * * *"
        assert str(d.start_date.tzinfo) == "UTC"
        assert d.max_active_runs == 1
    assert live.catchup is False
    assert fixture_dag.catchup is True
    assert (fixture_dag.end_date - fixture_dag.start_date).days + 1 == 7

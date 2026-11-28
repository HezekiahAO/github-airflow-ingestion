from datetime import datetime, timezone

import pytest

from github_ingestion import db
from github_ingestion.schemas import GitHubRepo

RUN1, RUN2, RUN3 = "manual__run1", "manual__run2", "manual__run3"


def repo(raw_repo, **changes) -> dict:
    return GitHubRepo.model_validate({**raw_repo, **changes}).model_dump(mode="json")


def bad(raw_repo, **changes) -> dict:
    return {"org": "stripe", "repo_id": changes.get("id", raw_repo["id"]), "raw_payload": {**raw_repo, **changes},
            "errors": [{"field": "forks_count", "message": "must be >= 0"}], "source_file": "raw/stripe/page_0001.json"}


def open_runs(conn, *run_ids):
    for run_id in run_ids:
        db.start_run(conn, run_id, "github_ingestion", "fixture", ["stripe"])


def scalar(conn, query, params=()):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()[0]


def test_migrate_is_idempotent(pg_conn):
    assert db.migrate(pg_conn, scalar(pg_conn, "SELECT current_schema()")) == ["001_init.sql"]


def test_first_load_inserts(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1)
    counts = db.load_batch(pg_conn, RUN1, [repo(raw_repo), repo(raw_repo, id=2, name="b", full_name="stripe/b")], [])
    assert counts["records_inserted"] == 2
    assert scalar(pg_conn, "SELECT count(*) FROM github_repos") == 2


def test_reloading_same_data_is_idempotent(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1, RUN2)
    db.load_batch(pg_conn, RUN1, [repo(raw_repo)], [bad(raw_repo, forks_count=-1)])
    counts = db.load_batch(pg_conn, RUN2, [repo(raw_repo)], [bad(raw_repo, forks_count=-1)])

    assert counts["records_inserted"] == 0 and counts["records_unchanged"] == 1
    assert counts["quarantine_new"] == 0
    assert scalar(pg_conn, "SELECT count(*) FROM github_repos") == 1
    assert scalar(pg_conn, "SELECT count(*) FROM github_repos_quarantine") == 1
    assert scalar(pg_conn, "SELECT times_seen FROM github_repos_quarantine") == 2
    assert scalar(pg_conn, "SELECT last_seen_run_id FROM github_repos") == RUN2


def test_retrying_the_same_run_does_not_inflate_counts(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1)
    db.load_batch(pg_conn, RUN1, [repo(raw_repo)], [bad(raw_repo, forks_count=-1)])
    db.load_batch(pg_conn, RUN1, [repo(raw_repo)], [bad(raw_repo, forks_count=-1)])
    assert scalar(pg_conn, "SELECT times_seen FROM github_repos_quarantine") == 1


def test_changed_record_is_updated(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1, RUN2)
    db.load_batch(pg_conn, RUN1, [repo(raw_repo)], [])
    counts = db.load_batch(pg_conn, RUN2, [repo(raw_repo, stargazers_count=9999, updated_at="2026-11-21T00:00:00Z")], [])
    assert counts["records_updated"] == 1
    assert scalar(pg_conn, "SELECT stargazers_count FROM github_repos") == 9999
    assert scalar(pg_conn, "SELECT first_seen_run_id FROM github_repos") == RUN1


def test_older_snapshot_never_overwrites_newer_data(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1, RUN2)
    db.load_batch(pg_conn, RUN1, [repo(raw_repo, stargazers_count=2000, updated_at="2026-11-22T00:00:00Z")], [])
    counts = db.load_batch(pg_conn, RUN2, [repo(raw_repo, stargazers_count=1, updated_at="2026-11-01T00:00:00Z")], [])
    assert counts["records_stale_skipped"] == 1
    assert scalar(pg_conn, "SELECT stargazers_count FROM github_repos") == 2000


def test_duplicate_repo_in_batch_keeps_newest(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1)
    db.load_batch(pg_conn, RUN1, [repo(raw_repo, stargazers_count=5, updated_at="2026-11-21T00:00:00Z"), repo(raw_repo)], [])
    assert scalar(pg_conn, "SELECT stargazers_count FROM github_repos") == 5


def test_checkpoint_only_moves_forward(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1, RUN2)
    db.load_batch(pg_conn, RUN1, [repo(raw_repo, updated_at="2026-11-22T00:00:00Z")], [])
    db.load_batch(pg_conn, RUN2, [repo(raw_repo, id=2, name="b", full_name="stripe/b", updated_at="2026-11-02T00:00:00Z")], [])
    assert db.get_checkpoints(pg_conn, ["stripe"]) == {"stripe": datetime(2026, 11, 22, tzinfo=timezone.utc)}


def test_full_refresh_soft_deletes_missing_and_restores_returning(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1, RUN2, RUN3)
    other = repo(raw_repo, id=2, name="b", full_name="stripe/b")
    db.load_batch(pg_conn, RUN1, [repo(raw_repo), other], [])
    counts = db.load_batch(pg_conn, RUN2, [repo(raw_repo)], [], full_refresh_orgs={"stripe"})
    assert counts["records_soft_deleted"] == 1
    assert scalar(pg_conn, "SELECT is_deleted FROM github_repos WHERE repo_id = 2") is True
    db.load_batch(pg_conn, RUN3, [repo(raw_repo), other], [], full_refresh_orgs={"stripe"})
    assert scalar(pg_conn, "SELECT is_deleted FROM github_repos WHERE repo_id = 2") is False


def test_incremental_load_does_not_soft_delete(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1, RUN2)
    db.load_batch(pg_conn, RUN1, [repo(raw_repo), repo(raw_repo, id=2, name="b", full_name="stripe/b")], [])
    db.load_batch(pg_conn, RUN2, [repo(raw_repo)], [])
    assert scalar(pg_conn, "SELECT count(*) FROM github_repos WHERE is_deleted") == 0


def test_failed_load_rolls_back_everything(pg_conn, raw_repo):
    open_runs(pg_conn, RUN1)
    with pytest.raises(Exception):
        # Unknown run id violates the foreign key, after repos were already written.
        db.load_batch(pg_conn, "missing-run", [repo(raw_repo)], [bad(raw_repo, forks_count=-1)])
    assert scalar(pg_conn, "SELECT count(*) FROM github_repos") == 0
    assert scalar(pg_conn, "SELECT count(*) FROM ingestion_checkpoints") == 0


def test_run_audit_lifecycle(pg_conn):
    open_runs(pg_conn, RUN1)
    assert scalar(pg_conn, "SELECT status FROM pipeline_runs") == "running"
    db.finish_run(pg_conn, RUN1, "failed", {"records_extracted": 10}, failed_tasks=["load_and_quarantine"], error_message="boom")
    assert scalar(pg_conn, "SELECT status FROM pipeline_runs") == "failed"
    assert scalar(pg_conn, "SELECT records_extracted FROM pipeline_runs") == 10
    assert scalar(pg_conn, "SELECT ended_at IS NOT NULL FROM pipeline_runs") is True
    open_runs(pg_conn, RUN1)  # cleared and re-run in Airflow
    assert scalar(pg_conn, "SELECT status FROM pipeline_runs") == "running"
    assert scalar(pg_conn, "SELECT error_message FROM pipeline_runs") is None


def test_status_must_be_known(pg_conn):
    open_runs(pg_conn, RUN1)
    with pytest.raises(Exception):
        db.finish_run(pg_conn, RUN1, "kinda-ok")

from datetime import datetime, timezone

import pytest

from github_ingestion import db, quality, storage
from github_ingestion.schemas import validate_records

NOW = datetime(2026, 11, 25, tzinfo=timezone.utc)


def summarise(tmp_path, raw_records, org="stripe"):
    valid_models, invalid = validate_records(raw_records)
    valid = [m.model_dump(mode="json") for m in valid_models]
    for entry in invalid:
        entry["org"] = org
    return valid, invalid, {
        "org": org,
        "valid_path": str(storage.write_jsonl(tmp_path / "valid.jsonl", valid)),
        "invalid_path": str(storage.write_jsonl(tmp_path / "invalid.jsonl", invalid)),
        "distinct_repo_ids": len({r.get("id") for r in raw_records}),
    }


def test_metric_value_and_pass():
    m = quality._metric("validity", 9, 10)
    assert m.value == 0.9 and m.passed
    assert quality._metric("uniqueness", 0, 0).value == 1.0


@pytest.mark.parametrize("changes, accurate", [
    ({}, True),
    ({"html_url": "https://github.com/stripe/other"}, False),
    ({"created_at": "2005-01-01T00:00:00Z"}, False),
    ({"updated_at": "2030-01-01T00:00:00Z"}, False),
    ({"pushed_at": "2030-01-01T00:00:00Z"}, False),
])
def test_accuracy_rules(raw_repo, changes, accurate):
    (model,), _ = validate_records([{**raw_repo, **changes}])
    assert quality.is_accurate(model.model_dump(mode="json"), NOW) is accurate


def test_report_flags_errors_and_warnings():
    metrics = [quality._metric("completeness", 98, 100), quality._metric("uniqueness", 9, 10),
               quality._metric("validity", 10, 10)]
    report = quality.build_report("r1", metrics)
    assert report["passed"] is False
    assert report["failed_errors"] == ["completeness"] and report["failed_warnings"] == ["uniqueness"]


def test_quality_end_to_end(tmp_path, raw_repo, pg_conn):
    records = [raw_repo, {**raw_repo, "id": 2, "name": "b", "full_name": "stripe/b",
                          "html_url": "https://github.com/stripe/b"},
               raw_repo,  # duplicate
               {**raw_repo, "id": 3, "html_url": None}]  # invalid + incomplete
    valid, invalid, summary = summarise(tmp_path, records)
    db.start_run(pg_conn, "r1", "github_ingestion", "fixture", ["stripe"])
    db.load_batch(pg_conn, "r1", valid, invalid)
    report = quality.run_quality_checks(pg_conn, "r1", [summary], tmp_path / "report.json", NOW)
    m = report["metrics"]
    assert m["validity"]["value"] == 0.75
    assert m["uniqueness"]["numerator"] == 3 and m["uniqueness"]["denominator"] == 4
    assert m["completeness"]["numerator"] == 27 and m["completeness"]["denominator"] == 28
    assert m["consistency"]["value"] == 1.0 and m["accuracy"]["value"] == 1.0
    assert report["failed_errors"] == ["completeness"]
    assert storage.read_json(tmp_path / "report.json") == report
    # re-running (e.g. a task retry) overwrites rather than duplicating rows
    quality.run_quality_checks(pg_conn, "r1", [summary], tmp_path / "report.json", NOW)
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM data_quality_metrics WHERE run_id = 'r1'")
        assert cur.fetchone()[0] == 5


def test_consistency_detects_drift(tmp_path, raw_repo, pg_conn):
    valid, invalid, summary = summarise(tmp_path, [raw_repo])
    db.start_run(pg_conn, "r1", "github_ingestion", "fixture", ["stripe"])
    db.load_batch(pg_conn, "r1", valid, invalid)
    with pg_conn.cursor() as cur:
        cur.execute("UPDATE github_repos SET stargazers_count = 0")
    pg_conn.commit()
    (consistency,) = [m for m in quality.compute_metrics(pg_conn, [summary], NOW) if m.metric == "consistency"]
    assert consistency.value == 0.0


def test_consistency_accepts_newer_version_in_db(tmp_path, raw_repo, pg_conn):
    newer = {**raw_repo, "updated_at": "2026-11-24T00:00:00Z", "stargazers_count": 9999}
    db.start_run(pg_conn, "r1", "github_ingestion", "fixture", ["stripe"])
    db.load_batch(pg_conn, "r1", [validate_records([newer])[0][0].model_dump(mode="json")], [])
    valid, invalid, summary = summarise(tmp_path, [raw_repo])  # older snapshot replayed
    db.start_run(pg_conn, "r2", "github_ingestion", "fixture", ["stripe"])
    db.load_batch(pg_conn, "r2", valid, invalid)
    (consistency,) = [m for m in quality.compute_metrics(pg_conn, [summary], NOW) if m.metric == "consistency"]
    assert consistency.value == 1.0

import json

import pytest

from github_ingestion import pipeline, storage
from github_ingestion.github_client import Page


class FakeSource:
    def __init__(self, pages: dict[str, list[list]]):
        self.pages = pages

    def iter_org_repo_pages(self, org, per_page=100, sort="full_name"):
        for i, body in enumerate(self.pages[org], start=1):
            yield Page(org=org, page_number=i, url=f"fake://{org}/{i}", raw_text=json.dumps(body))


def make(raw_repo, **changes):
    return {**raw_repo, **changes}


def test_extract_writes_raw_pages_and_manifest(tmp_path, raw_repo):
    source = FakeSource({"stripe": [[raw_repo, make(raw_repo, id=2)], [make(raw_repo, id=3)]]})
    manifest = pipeline.extract_org(source, "stripe", tmp_path)
    assert manifest["pages_fetched"] == 2 and manifest["records_extracted"] == 3
    assert storage.read_json(tmp_path / "raw" / "stripe" / "manifest.json") == manifest
    assert json.loads((tmp_path / "raw" / "stripe" / "page_0001.json").read_text())[0]["id"] == 123


def test_extract_retry_removes_pages_from_previous_attempt(tmp_path, raw_repo):
    pipeline.extract_org(FakeSource({"stripe": [[raw_repo]] * 3}), "stripe", tmp_path)
    pipeline.extract_org(FakeSource({"stripe": [[raw_repo]]}), "stripe", tmp_path)
    assert sorted(p.name for p in (tmp_path / "raw" / "stripe").iterdir()) == ["manifest.json", "page_0001.json"]


def test_validate_splits_and_tracks_source_file(tmp_path, raw_repo):
    bad = make(raw_repo, id=2, forks_count=-1)
    manifest = pipeline.extract_org(FakeSource({"stripe": [[raw_repo], [bad]]}), "stripe", tmp_path)
    summary = pipeline.validate_org(manifest, tmp_path)
    assert (summary["records_valid"], summary["records_invalid"]) == (1, 1)
    (invalid,) = storage.read_jsonl(summary["invalid_path"])
    assert invalid["org"] == "stripe" and invalid["source_file"].endswith("page_0002.json")
    assert invalid["raw_payload"] == bad


def test_validate_counts_duplicates(tmp_path, raw_repo):
    manifest = pipeline.extract_org(FakeSource({"stripe": [[raw_repo], [raw_repo]]}), "stripe", tmp_path)
    summary = pipeline.validate_org(manifest, tmp_path)
    assert summary["records_extracted"] == 2 and summary["distinct_repo_ids"] == 1


def test_validate_rejects_non_list_page(tmp_path):
    org_dir = tmp_path / "raw" / "stripe"
    path = storage.write_raw_page(org_dir, 1, '{"message": "Not Found"}')
    manifest = {"org": "stripe", "files": [str(path)], "pages_fetched": 1, "records_extracted": 0, "full_refresh": True}
    with pytest.raises(ValueError, match="expected a JSON list"):
        pipeline.validate_org(manifest, tmp_path)


def test_load_results_end_to_end(tmp_path, raw_repo, pg_conn):
    from github_ingestion import db
    db.start_run(pg_conn, "run1", "github_ingestion", "fixture", ["stripe"])
    manifest = pipeline.extract_org(FakeSource({"stripe": [[raw_repo, make(raw_repo, id=2, forks_count=-1)]]}), "stripe", tmp_path)
    counts = pipeline.load_results(pg_conn, "run1", [pipeline.validate_org(manifest, tmp_path)])
    assert counts["records_inserted"] == 1 and counts["records_quarantined"] == 1
    assert counts["records_extracted"] == 2


# ------------------------------------------------------------ incremental logic
from datetime import datetime, timezone  # noqa: E402

UTC = timezone.utc


def test_plan_full_refresh_when_no_checkpoint():
    plan = pipeline.plan_extraction(["stripe", "shopify"], {"stripe": datetime(2026, 11, 20, tzinfo=UTC)},
                                    datetime(2026, 11, 19, tzinfo=UTC))
    assert plan == {"stripe": "2026-11-20T00:00:00+00:00", "shopify": None}


def test_plan_full_refresh_on_monday_or_when_forced():
    cp = {"stripe": datetime(2026, 11, 20, tzinfo=UTC)}
    assert pipeline.plan_extraction(["stripe"], cp, datetime(2026, 11, 23, tzinfo=UTC)) == {"stripe": None}
    assert pipeline.plan_extraction(["stripe"], cp, datetime(2026, 11, 19, tzinfo=UTC), True) == {"stripe": None}


def test_incremental_stops_at_first_page_older_than_checkpoint(tmp_path, raw_repo):
    new = [make(raw_repo, id=i, updated_at="2026-11-22T00:00:00Z") for i in (1, 2)]
    old = [make(raw_repo, id=i, updated_at="2026-11-01T00:00:00Z") for i in (3, 4)]
    calls = []

    class Source(FakeSource):
        def iter_org_repo_pages(self, org, per_page=100, sort="full_name"):
            calls.append(sort)
            for page in super().iter_org_repo_pages(org, per_page):
                calls.append(page.page_number)
                yield page

    source = Source({"stripe": [new, old, old, old]})
    manifest = pipeline.extract_org(source, "stripe", tmp_path, since="2026-11-20T00:00:00+00:00")
    assert calls == ["updated", 1, 2]  # page 3 and 4 are never requested
    assert manifest["full_refresh"] is False and manifest["records_extracted"] == 4


def test_fixture_source_paginates_and_sorts(tmp_path, raw_repo):
    from github_ingestion.fixtures import FixtureSource, SnapshotNotFound, missing_snapshots
    folder = tmp_path / "snapshots" / "2026-11-18"
    folder.mkdir(parents=True)
    rows = [make(raw_repo, id=i, updated_at=f"2026-11-1{i}T00:00:00Z") for i in range(1, 8)]
    (folder / "stripe.json").write_text(json.dumps(rows))
    src = FixtureSource(tmp_path, "2026-11-18", page_size=3)
    pages = list(src.iter_org_repo_pages("stripe", sort="updated"))
    assert [len(p.records) for p in pages] == [3, 3, 1]
    assert pages[0].records[0]["id"] == 7
    assert missing_snapshots(tmp_path, "2026-11-18", ["stripe", "shopify"]) == ["shopify"]
    with pytest.raises(SnapshotNotFound):
        list(src.iter_org_repo_pages("shopify"))


def test_generated_fixtures_cover_seven_days():
    from pathlib import Path
    snaps = sorted(p.name for p in (Path(__file__).resolve().parents[1] / "fixtures" / "snapshots").iterdir())
    assert len(snaps) >= 7

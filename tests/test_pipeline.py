import json

import pytest

from github_ingestion import pipeline, storage
from github_ingestion.github_client import Page


class FakeSource:
    def __init__(self, pages: dict[str, list[list]]):
        self.pages = pages

    def iter_org_repo_pages(self, org, per_page=100):
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

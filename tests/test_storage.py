from github_ingestion import storage


def test_safe_segment_replaces_unsafe_characters():
    assert storage.safe_segment("manual__2026-11-27T10:00:00+00:00") == "manual__2026-11-27T10_00_00_00_00"


def test_run_dir_layout(tmp_path):
    path = storage.run_dir("github_ingestion", "scheduled__2026-11-01T06:00:00+00:00", base=tmp_path)
    assert path == tmp_path / "github_ingestion" / "scheduled__2026-11-01T06_00_00_00_00"


def test_raw_page_is_written_unmodified(tmp_path):
    body = '[{"id": 1,   "x": "kept as-is"}]'
    path = storage.write_raw_page(tmp_path / "stripe", 3, body)
    assert path.name == "page_0003.json"
    assert path.read_text() == body
    assert not list(tmp_path.rglob("*.tmp"))


def test_reset_dir_removes_files_from_earlier_attempt(tmp_path):
    org_dir = tmp_path / "raw" / "stripe"
    storage.write_raw_page(org_dir, 1, "[]")
    storage.write_raw_page(org_dir, 2, "[]")
    storage.reset_dir(org_dir)
    assert list(org_dir.iterdir()) == []


def test_jsonl_round_trip(tmp_path):
    rows = [{"a": 1}, {"b": [1, 2]}]
    path = storage.write_jsonl(tmp_path / "x.jsonl", rows)
    assert storage.read_jsonl(path) == rows


def test_json_round_trip(tmp_path):
    path = storage.write_json(tmp_path / "m.json", {"org": "stripe", "files": ["a"]})
    assert storage.read_json(path) == {"files": ["a"], "org": "stripe"}

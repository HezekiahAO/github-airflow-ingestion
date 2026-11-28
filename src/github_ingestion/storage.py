"""File storage for raw API pages and intermediate task outputs.

Everything lives under one run directory on a volume shared by all Airflow
tasks, e.g. data/github_ingestion/manual__2026-11-27T10_00_00_00_00/:

    raw/<org>/page_0001.json      unmodified GitHub response bodies
    raw/<org>/manifest.json       what extraction produced (paths, counts)
    processed/<org>/valid.jsonl   validated records
    processed/<org>/invalid.jsonl records to quarantine, with errors
    reports/quality_report.json

Tasks pass only these paths through XCom, never the payloads themselves.
"""
import json
import os
import re
import shutil
from pathlib import Path
from typing import Iterable

DEFAULT_DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "data"))


def safe_segment(value: str) -> str:
    """Make a run id like 'manual__2026-11-27T10:00:00+00:00' safe for a path."""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")


def run_dir(dag_id: str, run_id: str, base: Path | str = DEFAULT_DATA_DIR) -> Path:
    return Path(base) / safe_segment(dag_id) / safe_segment(run_id)


def raw_org_dir(run_path: Path, org: str) -> Path:
    return run_path / "raw" / safe_segment(org)


def processed_org_dir(run_path: Path, org: str) -> Path:
    return run_path / "processed" / safe_segment(org)


def reset_dir(path: Path) -> Path:
    """Empty a directory so a retried task never mixes files from an earlier attempt."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


def atomic_write_text(path: Path, text: str) -> Path:
    """Write to a temp file then rename, so readers never see a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)
    return path


def write_raw_page(org_dir: Path, page_number: int, raw_text: str) -> Path:
    return atomic_write_text(org_dir / f"page_{page_number:04d}.json", raw_text)


def write_json(path: Path, obj) -> Path:
    return atomic_write_text(path, json.dumps(obj, indent=2, sort_keys=True, default=str))


def read_json(path: Path | str):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_jsonl(path: Path, rows: Iterable[dict]) -> Path:
    return atomic_write_text(path, "".join(json.dumps(r, default=str) + "\n" for r in rows))


def read_jsonl(path: Path | str) -> list[dict]:
    with Path(path).open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]

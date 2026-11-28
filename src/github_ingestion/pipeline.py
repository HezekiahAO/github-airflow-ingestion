"""Task-level pipeline logic, kept free of Airflow so it can be unit tested.

The DAG calls these functions; each one reads its inputs from files/DB and
returns a small JSON-serialisable summary that Airflow stores in XCom.
"""
import logging
from pathlib import Path
from typing import Iterator, Protocol

from github_ingestion import db, storage
from github_ingestion.github_client import Page
from github_ingestion.schemas import validate_records

logger = logging.getLogger(__name__)


class RepoSource(Protocol):
    """Anything that yields pages of repositories: the live GitHub client or a fixture snapshot."""

    def iter_org_repo_pages(self, org: str, per_page: int = 100) -> Iterator[Page]: ...


def extract_org(source: RepoSource, org: str, run_path: Path) -> dict:
    """Fetch every page for one org and save each response body unmodified."""
    org_dir = storage.reset_dir(storage.raw_org_dir(run_path, org))
    files, records = [], 0
    for page in source.iter_org_repo_pages(org):
        files.append(str(storage.write_raw_page(org_dir, page.page_number, page.raw_text)))
        records += len(page.records)
    manifest = {"org": org, "files": files, "pages_fetched": len(files),
                "records_extracted": records, "full_refresh": True}
    storage.write_json(org_dir / "manifest.json", manifest)
    logger.info("Extracted %d records in %d page(s) for %s", records, len(files), org)
    return manifest


def validate_org(manifest: dict, run_path: Path) -> dict:
    """Validate the raw pages of one org and write valid/invalid JSONL files."""
    org = manifest["org"]
    raw: list[tuple[dict, str]] = []
    for path in manifest["files"]:
        body = storage.read_json(path)
        if not isinstance(body, list):
            raise ValueError(f"{path}: expected a JSON list of repositories, got {type(body).__name__}")
        raw.extend((record, path) for record in body)

    valid_models, invalid = validate_records([r for r, _ in raw])
    source_by_id = {id(r): p for r, p in raw}
    for entry in invalid:
        entry["org"] = org
        entry["source_file"] = source_by_id.get(id(entry["raw_payload"]))

    out_dir = storage.reset_dir(storage.processed_org_dir(run_path, org))
    valid = [m.model_dump(mode="json") for m in valid_models]
    valid_path = storage.write_jsonl(out_dir / "valid.jsonl", valid)
    invalid_path = storage.write_jsonl(out_dir / "invalid.jsonl", invalid)
    distinct_ids = {r.get("id") for r, _ in raw if isinstance(r, dict) and r.get("id") is not None}
    summary = {
        "org": org,
        "valid_path": str(valid_path),
        "invalid_path": str(invalid_path),
        "pages_fetched": manifest["pages_fetched"],
        "records_extracted": len(raw),
        "records_valid": len(valid),
        "records_invalid": len(invalid),
        "distinct_repo_ids": len(distinct_ids),
        "full_refresh": manifest["full_refresh"],
    }
    logger.info("Validated %s: %d valid, %d invalid", org, len(valid), len(invalid))
    return summary


def load_results(conn, run_id: str, summaries: list[dict]) -> dict:
    """Load every org's validated files in a single transaction."""
    valid, invalid = [], []
    for s in summaries:
        valid.extend(storage.read_jsonl(s["valid_path"]))
        invalid.extend(storage.read_jsonl(s["invalid_path"]))
    full_refresh_orgs = {s["org"] for s in summaries if s["full_refresh"]}
    counts = db.load_batch(conn, run_id, valid, invalid, full_refresh_orgs)
    for key in ("pages_fetched", "records_extracted", "records_valid", "records_invalid"):
        counts[key] = sum(s[key] for s in summaries)
    logger.info("Load counts: %s", counts)
    return counts

"""Task-level pipeline logic, kept free of Airflow so it can be unit tested.

The DAG calls these functions; each one reads its inputs from files/DB and
returns a small JSON-serialisable summary that Airflow stores in XCom.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator, Protocol

from github_ingestion import db, storage
from github_ingestion.github_client import Page
from github_ingestion.schemas import validate_records

logger = logging.getLogger(__name__)


class RepoSource(Protocol):
    """Anything that yields pages of repositories: the live GitHub client or a fixture snapshot."""

    def iter_org_repo_pages(self, org: str, per_page: int = 100, sort: str = "full_name") -> Iterator[Page]: ...


def parse_ts(value) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def plan_extraction(orgs: list[str], checkpoints: dict[str, datetime], interval_start: datetime | None,
                    force_full_refresh: bool = False, full_refresh_weekday: int = 0) -> dict[str, str | None]:
    """Decide per org: full refresh (None) or incremental since the checkpoint (ISO timestamp).

    Full refresh when forced, when the org has never been loaded, or on the weekly
    reconciliation day (Monday by default) -- only a full listing can prove that a
    repository disappeared, so soft deletes happen on those runs only.
    """
    weekly = interval_start is not None and interval_start.weekday() == full_refresh_weekday
    return {
        org: None if (force_full_refresh or weekly or org not in checkpoints) else checkpoints[org].isoformat()
        for org in orgs
    }


def _page_is_older_than(records: list, since: datetime) -> bool:
    stamps = [parse_ts(r.get("updated_at")) for r in records if isinstance(r, dict)]
    return all(ts is not None and ts < since for ts in stamps)


def extract_org(source: RepoSource, org: str, run_path: Path, since: str | None = None) -> dict:
    """Fetch the pages for one org and save each response body unmodified.

    since=None  -> full refresh: every page, alphabetical order.
    since=<iso> -> incremental: newest-updated first, stop after the first page whose
                   repos are all older than the checkpoint (no further API calls).
    """
    since_dt = parse_ts(since) if since else None
    org_dir = storage.reset_dir(storage.raw_org_dir(run_path, org))
    files, records = [], 0
    for page in source.iter_org_repo_pages(org, sort="updated" if since_dt else "full_name"):
        files.append(str(storage.write_raw_page(org_dir, page.page_number, page.raw_text)))
        body = page.records
        records += len(body) if isinstance(body, list) else 0
        if since_dt and isinstance(body, list) and _page_is_older_than(body, since_dt):
            break
    manifest = {"org": org, "files": files, "pages_fetched": len(files), "records_extracted": records,
                "full_refresh": since_dt is None, "since": since}
    storage.write_json(org_dir / "manifest.json", manifest)
    logger.info("Extracted %d records in %d page(s) for %s (%s)", records, len(files), org,
                "full refresh" if since_dt is None else f"incremental since {since}")
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

"""Replay saved GitHub API snapshots, so backfills over past logical dates are reproducible.

GitHub only returns the *current* state of each repository. A real backfill of
"what did stripe look like on 2026-11-18" is impossible from the live API, so
we keep one snapshot per day under fixtures/snapshots/<YYYY-MM-DD>/<org>.json
and replay it through the same code path as the live client.
"""
import json
from pathlib import Path
from typing import Iterator

from github_ingestion.github_client import GitHubAPIError, Page


class SnapshotNotFound(GitHubAPIError):
    """No snapshot exists for the requested date/org (the fixture equivalent of a 404)."""


def snapshot_dir(fixtures_dir: Path | str, snapshot_date: str) -> Path:
    return Path(fixtures_dir) / "snapshots" / snapshot_date


def missing_snapshots(fixtures_dir: Path | str, snapshot_date: str, orgs: list[str]) -> list[str]:
    folder = snapshot_dir(fixtures_dir, snapshot_date)
    return [org for org in orgs if not (folder / f"{org}.json").is_file()]


class FixtureSource:
    """Same interface as GitHubClient.iter_org_repo_pages, backed by a snapshot file."""

    def __init__(self, fixtures_dir: Path | str, snapshot_date: str, page_size: int = 3):
        self.folder = snapshot_dir(fixtures_dir, snapshot_date)
        self.page_size = page_size

    def iter_org_repo_pages(self, org: str, per_page: int = 100, sort: str = "full_name") -> Iterator[Page]:
        path = self.folder / f"{org}.json"
        if not path.is_file():
            raise SnapshotNotFound(f"No snapshot for org '{org}' at {path}")
        records = json.loads(path.read_text(encoding="utf-8"))
        if sort == "updated":
            records = sorted(records, key=lambda r: str(r.get("updated_at") or ""), reverse=True)
        size = min(per_page, self.page_size)
        for i in range(0, max(len(records), 1), size):
            yield Page(org=org, page_number=i // size + 1, url=f"file://{path}#page={i // size + 1}",
                       raw_text=json.dumps(records[i:i + size], indent=2))

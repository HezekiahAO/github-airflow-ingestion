"""PostgreSQL persistence: migrations, audit rows, idempotent loading and checkpoints.

Every function takes an open psycopg2 connection, so the same code runs in
Airflow (via PostgresHook.get_conn()) and in tests (via psycopg2.connect()).
"""
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

from psycopg2 import sql
from psycopg2.extras import Json, execute_values

SQL_DIR = Path(os.environ.get("SQL_DIR", Path(__file__).resolve().parents[2] / "sql"))

REPO_COLUMNS = [
    "repo_id", "org", "name", "full_name", "html_url", "description", "language", "license",
    "default_branch", "visibility", "is_fork", "is_archived", "stargazers_count", "forks_count",
    "open_issues_count", "watchers_count", "created_at", "updated_at", "pushed_at",
]


def canonical_hash(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def use_schema(conn, schema: str) -> None:
    """Create the schema if needed and make it the default for this connection."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))


def migrate(conn, schema: str = "public") -> list[str]:
    """Apply every sql/*.sql file in order. Files are idempotent, so re-running is safe."""
    use_schema(conn, schema)
    applied = []
    with conn.cursor() as cur:
        for path in sorted(SQL_DIR.glob("*.sql")):
            cur.execute(path.read_text())
            applied.append(path.name)
    conn.commit()
    return applied


# ---------------------------------------------------------------- audit (pipeline_runs)

def start_run(conn, run_id: str, dag_id: str, source_mode: str, orgs: list[str],
              logical_date: datetime | None = None, data_interval_start: datetime | None = None,
              data_interval_end: datetime | None = None) -> None:
    """Open (or re-open, when a run is cleared and re-run) the audit row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (run_id, dag_id, source_mode, orgs, logical_date,
                                       data_interval_start, data_interval_end, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'running')
            ON CONFLICT (run_id) DO UPDATE SET
                status = 'running', started_at = now(), ended_at = NULL,
                failed_tasks = NULL, error_message = NULL, orgs = EXCLUDED.orgs
            """,
            (run_id, dag_id, source_mode, orgs, logical_date, data_interval_start, data_interval_end),
        )
    conn.commit()


def ensure_run(conn, run_id: str, dag_id: str, source_mode: str, orgs: list[str]) -> None:
    """Create the audit row if start_run never got to (e.g. start_run itself failed)."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (run_id, dag_id, source_mode, orgs, status) "
            "VALUES (%s, %s, %s, %s, 'running') ON CONFLICT (run_id) DO NOTHING",
            (run_id, dag_id, source_mode, orgs),
        )
    conn.commit()


AUDIT_COUNT_FIELDS = [
    "pages_fetched", "records_extracted", "records_valid", "records_invalid",
    "records_inserted", "records_updated", "records_unchanged", "records_quarantined",
]


def finish_run(conn, run_id: str, status: str, counts: dict | None = None,
               quality_report: dict | None = None, failed_tasks: list[str] | None = None,
               error_message: str | None = None) -> None:
    counts = counts or {}
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "UPDATE pipeline_runs SET status = %s, ended_at = now(), quality_report = %s, "
                "failed_tasks = %s, error_message = %s, {} WHERE run_id = %s"
            ).format(sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(f)) for f in AUDIT_COUNT_FIELDS
            )),
            [status, Json(quality_report) if quality_report is not None else None,
             failed_tasks or None, error_message,
             *[counts.get(f) for f in AUDIT_COUNT_FIELDS], run_id],
        )
        if cur.rowcount != 1:
            raise RuntimeError(f"pipeline_runs row for run_id={run_id!r} not found")
    conn.commit()


# ---------------------------------------------------------------- checkpoints

def get_checkpoints(conn, orgs: list[str]) -> dict[str, datetime]:
    with conn.cursor() as cur:
        cur.execute("SELECT org, last_updated_at FROM ingestion_checkpoints WHERE org = ANY(%s)", (orgs,))
        return dict(cur.fetchall())


# ---------------------------------------------------------------- loading

def load_batch(conn, run_id: str, valid: list[dict], invalid: list[dict],
               full_refresh_orgs: set[str] = frozenset()) -> dict[str, int]:
    """Upsert valid repos, quarantine invalid ones and advance checkpoints in ONE transaction.

    Either everything for this run is committed or nothing is, so a crash
    halfway through can never leave repos loaded without their checkpoint.

    valid:   GitHubRepo.model_dump(mode="json") dicts
    invalid: {"org", "repo_id", "raw_payload", "errors", "source_file"} dicts
    full_refresh_orgs: orgs whose complete repo list was fetched this run; repos
        of those orgs not in `valid` are soft-deleted (is_deleted = true).
    """
    try:
        with conn.cursor() as cur:
            counts = _upsert_repos(cur, run_id, valid)
            counts.update(_quarantine(cur, run_id, invalid))
            counts["records_soft_deleted"] = _soft_delete_missing(cur, valid, full_refresh_orgs)
            _advance_checkpoints(cur, run_id, valid)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return counts


def _dedupe_latest(records: list[dict]) -> list[dict]:
    """Keep one record per repo_id: the one with the newest updated_at."""
    latest: dict[int, dict] = {}
    for r in records:
        cur = latest.get(r["repo_id"])
        if cur is None or str(r["updated_at"]) >= str(cur["updated_at"]):
            latest[r["repo_id"]] = r
    return list(latest.values())


def _upsert_repos(cur, run_id: str, valid: list[dict]) -> dict[str, int]:
    records = _dedupe_latest(valid)
    counts = {"records_inserted": 0, "records_updated": 0, "records_unchanged": 0, "records_stale_skipped": 0}
    if not records:
        return counts

    # Lock and read what we already have, to classify each incoming record.
    cur.execute(
        "SELECT repo_id, record_hash, updated_at FROM github_repos WHERE repo_id = ANY(%s) FOR UPDATE",
        ([r["repo_id"] for r in records],),
    )
    existing = {repo_id: (h, upd) for repo_id, h, upd in cur.fetchall()}

    rows = []
    for r in records:
        h = canonical_hash({c: r.get(c) for c in REPO_COLUMNS})
        if r["repo_id"] not in existing:
            counts["records_inserted"] += 1
        else:
            old_hash, old_updated = existing[r["repo_id"]]
            if datetime.fromisoformat(str(r["updated_at"]).replace("Z", "+00:00")) < old_updated:
                counts["records_stale_skipped"] += 1   # older snapshot than what we hold
            elif old_hash == h:
                counts["records_unchanged"] += 1
            else:
                counts["records_updated"] += 1
        rows.append(tuple(r.get(c) for c in REPO_COLUMNS) + (h, run_id, run_id))

    cols = REPO_COLUMNS + ["record_hash", "first_seen_run_id", "last_seen_run_id"]
    updates = [c for c in REPO_COLUMNS if c != "repo_id"]
    execute_values(
        cur,
        sql.SQL(
            """
            INSERT INTO github_repos ({cols}) VALUES %s
            ON CONFLICT (repo_id) DO UPDATE SET
                {updates},
                row_updated_at = CASE WHEN github_repos.record_hash <> EXCLUDED.record_hash
                                      THEN now() ELSE github_repos.row_updated_at END,
                record_hash = EXCLUDED.record_hash,
                last_seen_run_id = EXCLUDED.last_seen_run_id,
                last_seen_at = now(),
                is_deleted = false,
                deleted_detected_at = NULL
            WHERE EXCLUDED.updated_at >= github_repos.updated_at
            """
        ).format(
            cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
            updates=sql.SQL(", ").join(sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in updates),
        ).as_string(cur),
        rows,
    )
    return counts


def _quarantine(cur, run_id: str, invalid: list[dict]) -> dict[str, int]:
    by_hash = {canonical_hash(i["raw_payload"]): i for i in invalid}
    if not by_hash:
        return {"records_quarantined": 0, "quarantine_new": 0}
    results = execute_values(
        cur,
        """
        INSERT INTO github_repos_quarantine AS q
            (payload_hash, repo_id, org, raw_payload, validation_errors, source_file,
             first_seen_run_id, last_seen_run_id)
        VALUES %s
        ON CONFLICT (payload_hash) DO UPDATE SET
            times_seen = q.times_seen + CASE WHEN q.last_seen_run_id <> EXCLUDED.last_seen_run_id THEN 1 ELSE 0 END,
            last_seen_run_id = EXCLUDED.last_seen_run_id,
            last_seen_at = now(),
            validation_errors = EXCLUDED.validation_errors
        RETURNING (xmax = 0) AS inserted
        """,
        [
            (h, i.get("repo_id") if isinstance(i.get("repo_id"), int) else None, i["org"],
             Json(i["raw_payload"]), Json(i["errors"]), i.get("source_file"), run_id, run_id)
            for h, i in by_hash.items()
        ],
        fetch=True,
    )
    return {"records_quarantined": len(by_hash), "quarantine_new": sum(1 for (ins,) in results if ins)}


def _soft_delete_missing(cur, valid: list[dict], full_refresh_orgs: set[str]) -> int:
    deleted = 0
    for org in full_refresh_orgs:
        seen = [r["repo_id"] for r in valid if r["org"] == org]
        cur.execute(
            """
            UPDATE github_repos SET is_deleted = true, deleted_detected_at = now()
            WHERE org = %s AND NOT is_deleted AND NOT (repo_id = ANY(%s))
            """,
            (org, seen),
        )
        deleted += cur.rowcount
    return deleted


def _advance_checkpoints(cur, run_id: str, valid: list[dict]) -> None:
    newest: dict[str, str] = {}
    for r in valid:
        if str(r["updated_at"]) > newest.get(r["org"], ""):
            newest[r["org"]] = str(r["updated_at"])
    for org, ts in newest.items():
        # GREATEST: re-processing an older snapshot must never move the checkpoint backwards.
        cur.execute(
            """
            INSERT INTO ingestion_checkpoints (org, last_updated_at, last_run_id) VALUES (%s, %s, %s)
            ON CONFLICT (org) DO UPDATE SET
                last_updated_at = GREATEST(ingestion_checkpoints.last_updated_at, EXCLUDED.last_updated_at),
                last_run_id = EXCLUDED.last_run_id,
                checkpointed_at = now()
            """,
            (org, ts, run_id),
        )

"""Data quality metrics for one pipeline run.

Each metric is a ratio in [0, 1] with a threshold and a severity:
  error -> the run is marked failed (the data should not be trusted)
  warn  -> recorded and logged, the run still succeeds
"""
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from github_ingestion import db, storage

GITHUB_LAUNCH = datetime(2007, 10, 1, tzinfo=timezone.utc)
REQUIRED_RAW_FIELDS = ["id", "name", "full_name", "html_url", "created_at", "updated_at", "default_branch"]

DEFINITIONS = {
    "completeness": ("Share of required fields (id, name, full_name, html_url, created_at, updated_at, "
                     "default_branch) that are present and non-null across all extracted records", 0.99, "error"),
    "validity": ("Share of extracted records that pass the Pydantic schema and business rules", 0.90, "warn"),
    "uniqueness": ("Distinct repository ids / extracted records (duplicates come from pagination shifts)",
                   1.0, "warn"),
    "consistency": ("Share of valid repositories whose row in github_repos matches the validated record "
                    "(source-to-target reconciliation after the load)", 1.0, "error"),
    "accuracy": ("Share of valid records that are plausible against reference rules: html_url equals "
                 "https://github.com/<full_name>, created_at not before GitHub's launch (2007-10-01), "
                 "timestamps not in the future", 0.99, "warn"),
}


@dataclass
class Metric:
    metric: str
    numerator: int
    denominator: int
    threshold: float
    severity: str
    definition: str

    @property
    def value(self) -> float:
        return 1.0 if self.denominator == 0 else round(self.numerator / self.denominator, 5)

    @property
    def passed(self) -> bool:
        return self.value >= self.threshold

    def as_dict(self) -> dict:
        return {**asdict(self), "value": self.value, "passed": self.passed}


def _metric(name: str, numerator: int, denominator: int) -> Metric:
    definition, threshold, severity = DEFINITIONS[name]
    return Metric(name, numerator, denominator, threshold, severity, definition)


def _parse(value) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def is_accurate(r: dict, now: datetime) -> bool:
    future = now + timedelta(days=1)
    created, updated, pushed = _parse(r["created_at"]), _parse(r["updated_at"]), _parse(r.get("pushed_at"))
    return (
        r["html_url"].lower() == f"https://github.com/{r['full_name']}".lower()
        and created is not None and created >= GITHUB_LAUNCH
        and updated is not None and updated <= future
        and (pushed is None or pushed <= future)
    )


def _same_record(validated: dict, stored: dict | None) -> bool:
    if stored is None:
        return False
    if stored["updated_at"] > _parse(validated["updated_at"]):
        return True  # a newer version was loaded since (stale snapshot replay); the loader rightly kept it
    for col in db.REPO_COLUMNS:
        expected, actual = validated.get(col), stored[col]
        if col in ("created_at", "updated_at", "pushed_at"):
            expected = _parse(expected) if expected is not None else None
        if expected != actual:
            return False
    return True


def compute_metrics(conn, summaries: list[dict], now: datetime | None = None) -> list[Metric]:
    now = now or datetime.now(timezone.utc)
    valid, invalid = [], []
    for s in summaries:
        valid.extend(storage.read_jsonl(s["valid_path"]))
        invalid.extend(storage.read_jsonl(s["invalid_path"]))
    extracted = len(valid) + len(invalid)

    present = len(valid) * len(REQUIRED_RAW_FIELDS)  # a valid record has every required field by definition
    for entry in invalid:
        payload = entry["raw_payload"] if isinstance(entry["raw_payload"], dict) else {}
        present += sum(payload.get(f) is not None for f in REQUIRED_RAW_FIELDS)

    latest = {r["repo_id"]: r for r in valid}  # same rule as the loader: last occurrence wins
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(db.REPO_COLUMNS)} FROM github_repos WHERE repo_id = ANY(%s)", (list(latest),))
        stored = {row[0]: dict(zip(db.REPO_COLUMNS, row)) for row in cur.fetchall()}
    consistent = sum(_same_record(r, stored.get(rid)) for rid, r in latest.items())

    return [
        _metric("completeness", present, extracted * len(REQUIRED_RAW_FIELDS)),
        _metric("validity", len(valid), extracted),
        _metric("uniqueness", sum(s["distinct_repo_ids"] for s in summaries), extracted),
        _metric("consistency", consistent, len(latest)),
        _metric("accuracy", sum(is_accurate(r, now) for r in latest.values()), len(latest)),
    ]


def build_report(run_id: str, metrics: list[Metric]) -> dict:
    failed = [m for m in metrics if not m.passed]
    return {
        "run_id": run_id,
        "passed": not any(m.severity == "error" for m in failed),
        "failed_errors": [m.metric for m in failed if m.severity == "error"],
        "failed_warnings": [m.metric for m in failed if m.severity == "warn"],
        "metrics": {m.metric: m.as_dict() for m in metrics},
    }


def run_quality_checks(conn, run_id: str, summaries: list[dict], report_path, now: datetime | None = None) -> dict:
    """Compute, persist (DB + JSON file) and return the quality report."""
    metrics = compute_metrics(conn, summaries, now)
    report = build_report(run_id, metrics)
    db.save_quality_metrics(conn, run_id, [m.as_dict() for m in metrics])
    storage.write_json(report_path, report)
    return report

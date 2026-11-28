# github-airflow-ingestion
Daily GitHub repository metadata ingestion pipeline orchestrated with Apache Airflow and PostgreSQL.

It collects public repository metadata for `stripe`, `shopify` and `microsoft` (configurable), keeps every
API response unmodified, validates each record, loads valid records idempotently, quarantines invalid ones,
measures data quality and records an honest audit row for every run.

See [docs/architecture.md](docs/architecture.md) for the design and the reasoning behind it.

## Repository layout
| Path | What it is |
|---|---|
| `src/github_ingestion/schemas.py` | Pydantic contract for a repository record + business rules |
| `src/github_ingestion/github_client.py` | GitHub REST client: auth, pagination, retries, rate-limit detection |
| `src/github_ingestion/storage.py` | Raw/processed files on the shared `./data` volume (atomic writes) |
| `src/github_ingestion/db.py` | Migrations, idempotent upserts, quarantine, soft deletes, checkpoints, audit |
| `src/github_ingestion/pipeline.py` | Task logic (extract / validate / load), independent of Airflow |
| `src/github_ingestion/fixtures.py` | Replays saved daily snapshots for reproducible backfills |
| `src/github_ingestion/quality.py` | Completeness, validity, uniqueness, consistency, accuracy |
| `dags/github_ingestion_dag.py` | The two DAGs: `github_ingestion` (live) and `github_ingestion_fixture` |
| `sql/001_init.sql` | Tables, constraints and indexes |
| `fixtures/snapshots/` | 7 daily snapshots (2026-11-18 .. 2026-11-24), made by `scripts/generate_fixtures.py` |
| `docker-compose.yaml`, `Dockerfile`, `.env.example` | Local Airflow 3.3 + PostgreSQL 16 environment |
| `tests/` | Unit, database and DAG tests |

## Quick start
Requirements: Docker with Compose v2, ~4 GB RAM for Docker.

```bash
cp .env.example .env
# edit .env: GITHUB_TOKEN, AIRFLOW_JWT_SECRET, passwords.
# On Linux also: AIRFLOW_UID=$(id -u)
# Use passwords that are not common words: Airflow masks secret values in logs,
# so a password like "github" would turn every "github" in the logs into "***".
docker compose up -d --build          # first start runs airflow-init (DB migrations, admin user, pipeline tables)
docker compose ps                     # wait until every service is "healthy"
```
Open http://localhost:8080 and log in with `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD`.

Stop: `docker compose down`. Fresh start (deletes all data): `docker compose down -v && rm -rf data/github_ingestion* logs/*`.

### GitHub token
Create a fine-grained token (no extra permissions are needed for public metadata) or a classic token with no
scopes, and put it in `.env` as `GITHUB_TOKEN`. It is only read from the environment, is never written to
files or the database, and Airflow masks it in task logs.

## Running the pipeline
**Live run** (current GitHub state, schema `public`): unpause `github_ingestion` in the UI, or
```bash
docker compose exec airflow-scheduler airflow dags unpause github_ingestion
docker compose exec airflow-scheduler airflow dags trigger github_ingestion
```
Trigger with `{"full_refresh": true}` to force a complete listing (also soft-deletes repos that disappeared).

**7-day controlled backfill** (fixture snapshots, schema `fixture`):
```bash
docker compose exec airflow-scheduler airflow dags unpause github_ingestion_fixture   # catchup creates 7 runs
# or explicitly, and again later to prove re-runs create no duplicates:
docker compose exec airflow-scheduler airflow backfill create --dag-id github_ingestion_fixture \
    --from-date 2026-11-18 --to-date 2026-11-24 --reprocess-behavior completed --max-active-runs 1
```
`--max-active-runs 1` matters: backfill runs must be processed in date order so checkpoints and
incremental windows advance in sequence.

**Failure demos** (fixture DAG, any snapshot):
```bash
# transient: first extract attempt raises a simulated 503, the retry succeeds -> run "success"
airflow dags trigger github_ingestion_fixture --run-id demo_transient_retry --conf '{"simulate_failure":"transient","snapshot_date":"2026-11-24"}'
# permanent: validation fails on every attempt -> run "failed", pipeline_runs.failed_tasks = {validate_repositories}
airflow dags trigger github_ingestion_fixture --run-id demo_permanent_failure --conf '{"simulate_failure":"permanent","snapshot_date":"2026-11-24"}'
# sensor timeout: snapshot does not exist, wait_for_snapshot gives up after 2 minutes -> run "failed"
airflow dags trigger github_ingestion_fixture --run-id demo_missing_snapshot --conf '{"snapshot_date":"2026-12-01"}'
```
(prefix each with `docker compose exec airflow-scheduler`).

## Inspecting results
```bash
docker compose exec postgres psql -U github -d github_data
```
```sql
SET search_path TO fixture;   -- or public for live runs
SELECT run_id, status, records_extracted, records_valid, records_invalid, records_inserted,
       records_updated, records_unchanged, failed_tasks FROM pipeline_runs ORDER BY started_at;
SELECT metric, value, threshold, severity, passed FROM data_quality_metrics WHERE run_id = '<run_id>';
SELECT org, raw_payload->>'full_name', errors, times_seen FROM github_repos_quarantine;
SELECT org, count(*), count(*) FILTER (WHERE is_deleted) FROM github_repos GROUP BY org;
SELECT * FROM ingestion_checkpoints;
```
Raw responses: `data/<dag_id>/<run_id>/raw/<org>/page_0001.json`; quality report:
`.../reports/quality_report.json`.

## Tests
```bash
docker compose run --rm airflow-cli python -m pytest -q      # everything, incl. DAG + Postgres tests
```
Without Docker: `pip install -e ".[test]" && pytest -q` (DAG tests skip without Airflow; DB tests skip unless
`TEST_DATABASE_URL` points at a PostgreSQL database).

| File | Covers |
|---|---|
| `test_schemas.py` | valid/optional/missing fields, business rules, mixed batches |
| `test_github_client.py` | pagination via `Link` header, max_pages guard, 5xx/timeout retries with backoff, 404/401 fail fast, rate-limit detection, token never logged (all mocked with `responses`) |
| `test_storage.py` | raw pages stored byte-for-byte, atomic writes, path sanitising |
| `test_db.py` | idempotent migrations and upserts, quarantine dedup, stale snapshots, soft delete/restore, forward-only checkpoints, rollback, audit constraints |
| `test_pipeline.py` | extract/validate/load, retry cleanup, incremental early stop, extraction planning, fixture source |
| `test_quality.py` | each metric, thresholds/severity, persistence, retry overwrite, drift detection |
| `test_dag.py` | DAG import, task dependencies, trigger rule, mapping, schedule/catchup, retries/timeouts, sensors |

## Evidence
Screenshots of successful, failed, recovered, repeated and quality-checked runs are in
[docs/screenshots](docs/screenshots); database output is in [docs/evidence.md](docs/evidence.md).

# Architecture

## Flow
```
                ┌──────────── Airflow (orchestration only) ────────────┐
GitHub API ──►  start_run ─► plan_extraction ─► check_api_quota (sensor) ─► extract_repositories[org]
(or fixture                                     wait_for_snapshot (sensor, fixture DAG)      │ raw JSON pages
 snapshots)                                                                                  ▼ ./data/.../raw/<org>/
                validate_repositories[org] ─► load_and_quarantine ─► quality_report ─► finalize_run (all_done)
                  │ valid.jsonl / invalid.jsonl     │ Postgres            │ metrics          │ pipeline_runs
```
Airflow decides *when* and *in what order* things run, retries them and records state. All real work is in
plain Python modules under `src/github_ingestion/`, which are unit tested without Airflow.

## Tasks
| Task | Does | XCom output (small only) |
|---|---|---|
| `start_run` | inserts a `pipeline_runs` row with status `running` | org list |
| `plan_extraction` | per org: full refresh or incremental since its checkpoint | `[{"org", "since"}]` |
| `check_api_quota` | sensor; waits (reschedule mode) until GitHub quota >= `GITHUB_MIN_QUOTA` | remaining, reset time |
| `wait_for_snapshot` | sensor (fixture DAG); waits until the day's snapshot files exist | snapshot date |
| `extract_repositories` | one mapped task per org; follows pagination, writes each response unmodified | manifest: file paths, counts |
| `validate_repositories` | one mapped task per org; Pydantic validation, writes valid/invalid JSONL | file paths, counts |
| `load_and_quarantine` | one transaction: upsert valid, quarantine invalid, soft delete, advance checkpoints | load counts |
| `quality_report` | computes and stores 5 metrics; fails the run on an error-severity breach | pass/warnings |
| `finalize_run` | `trigger_rule="all_done"`: always runs, writes final status, counts, failed tasks, quality report | status |

Why files + XCom: XCom values live in Airflow's metadata database, so they must stay small. API payloads go
to the shared `./data` volume (mounted into every Airflow container), and only paths and counts travel
through XCom. Each run writes under its own `run_id` folder, and a retried task clears its folder first.

## Storage (PostgreSQL, `sql/001_init.sql`)
* `github_repos`: one row per `repo_id` (primary key). `INSERT ... ON CONFLICT (repo_id) DO UPDATE`, only
  when the content hash changed and the incoming `updated_at` is not older than the stored one. Audit
  columns: `first_seen_run_id`, `last_seen_run_id`, `record_hash`, `ingested_at`, `updated_in_db_at`,
  `is_deleted`, `deleted_detected_at`.
* `github_repos_quarantine`: invalid payload as JSONB + validation errors, deduplicated by payload hash
  (`times_seen` counts repeats).
* `pipeline_runs`: one row per DAG run: interval, source mode, status (`running|success|failed`), counters,
  failed tasks, error message, quality report. Constraints stop a finished run without `ended_at`.
* `ingestion_checkpoints`: newest `updated_at` loaded per org; only moves forward (`GREATEST`).
* `data_quality_metrics`: one row per run and metric.

The live DAG uses schema `public`; the fixture DAG uses schema `fixture`, so demos never mix with real data.

## Scheduling, intervals and incremental loading
* Timetable `CronDataIntervalTimetable("0 0 * * *", timezone="UTC")`: the run with logical date
  2026-11-18 covers `[2026-11-18 00:00, 2026-11-19 00:00)` UTC and starts after that interval ends.
* Live DAG: `start_date=2026-11-01`, `catchup=False`. The GitHub API only returns *current* state, so catching
  up past intervals would just fetch today's data seven times. Re-running a live date is current-state
  reprocessing, not history.
* Fixture DAG: `start_date=2026-11-18`, `end_date=2026-11-24`, `catchup=True`. Each run replays the snapshot
  for its `data_interval_start`, which makes a real 7-day backfill reproducible.
* Incremental: each org's checkpoint is the newest `updated_at` already loaded. Incremental runs ask GitHub
  for `sort=updated` (newest first) and stop after the first page whose repos are all older than the
  checkpoint, saving API calls. A full refresh runs when there is no checkpoint, when forced
  (`full_refresh` param), and every Monday. Only a full listing can prove a repo disappeared, so soft
  deletes happen only on full refreshes; a repo that reappears is restored.
* Idempotency: re-running any date gives the same table contents. Repeat rows count as `unchanged`, quarantine
  repeats increase `times_seen`, and checkpoints never move backwards.

## Reliability
| Failure | Handling |
|---|---|
| timeout, connection error, 5xx | client retries 4 attempts with 1s/2s/4s backoff, then the task fails and Airflow retries it (3 retries, exponential backoff from 1 min, max 15 min) |
| 404 / 401 | fail fast, since retrying won't help |
| rate limit (403/429 with remaining=0 or Retry-After) | client raises `RateLimitExceeded` with the reset time instead of sleeping; the quota sensor waits in reschedule mode before extraction; the task retries later |
| stuck task | `execution_timeout=20 min` per task, `dagrun_timeout=3 h` |
| missing upstream data | `wait_for_snapshot` pokes every 30 s, times out after 2 min (fixture) |
| any failure | `finalize_run` runs anyway (`all_done`), marks the run `failed` with the failed task names, then fails itself so the DAG run is also red |
| secrets | token only in an env var and request headers, `mask_secret` in Airflow logs, `__repr__` hides it; DB credentials in the Airflow connection env var |

## Data quality
| Metric | Definition | Threshold | Severity |
|---|---|---|---|
| completeness | required fields present and non-null / (records × required fields) | 0.99 | error |
| validity | valid records / extracted records | 0.90 | warn |
| uniqueness | distinct repo ids / extracted records | 1.00 | warn |
| consistency | valid repos whose `github_repos` row equals the validated record / valid repos | 1.00 | error |
| accuracy | valid repos passing reference checks (URL = github.com/full_name, created after GitHub launch, no future timestamps) / valid repos | 0.99 | warn |

`error` metrics fail the run; `warn` metrics are recorded and logged. Results go to `data_quality_metrics`,
`pipeline_runs.quality_report` and `reports/quality_report.json`.

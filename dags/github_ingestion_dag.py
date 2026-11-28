"""GitHub repository metadata ingestion.

Two DAGs share the same tasks and code path:
  github_ingestion          live GitHub API, schema `public`
  github_ingestion_fixture  replayed daily snapshots, schema `fixture` (reproducible backfills)

start_run -> plan_extraction -> check_api_quota | wait_for_snapshot (sensor) -> extract_repositories[org]
          -> validate_repositories[org] -> load_and_quarantine -> finalize_run

Big data (API responses, validated records) is written to files on the shared
./data volume; XCom only carries file paths, timestamps and counts.

Scheduling: daily at 00:00 UTC with CronDataIntervalTimetable, so each run covers
the interval [data_interval_start, data_interval_end) and starts after it ends.
The live API only knows *current* state, so the live DAG does not catch up;
historical intervals are replayed with the fixture DAG instead.

Reliability: every task retries with exponential backoff and has an execution
timeout; the sensors wait (in reschedule mode, freeing the worker slot) for API
quota / snapshot files; finalize_run uses trigger_rule="all_done" so a failed
run is always recorded as failed. Param `simulate_failure` demonstrates a
transient failure that recovers on retry, or a permanent one that ends the run.
"""
import logging
import os
from contextlib import closing
from datetime import timedelta

import pendulum
import psycopg2
from airflow.sdk import BaseHook, Param, PokeReturnValue, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowException, AirflowFailException
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.sdk.log import mask_secret
from airflow.timetables.interval import CronDataIntervalTimetable

from github_ingestion import db, pipeline, storage
from github_ingestion.fixtures import FixtureSource, missing_snapshots
from github_ingestion.github_client import GitHubAPIError, GitHubClient, RateLimitExceeded

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "github_postgres"
DEFAULT_ORGS = [o.strip() for o in os.environ.get("GITHUB_ORGS", "stripe,shopify,microsoft").split(",") if o.strip()]
MIN_API_QUOTA = int(os.environ.get("GITHUB_MIN_QUOTA", "200"))
FIXTURES_DIR = os.environ.get("FIXTURES_DIR", "/opt/airflow/fixtures")
DAILY_UTC = CronDataIntervalTimetable("0 0 * * *", timezone="UTC")


def _connect(schema: str):
    # Credentials come from the Airflow connection (env var AIRFLOW_CONN_GITHUB_POSTGRES).
    # We use psycopg2 directly because db.py is written for it; PostgresHook may hand back psycopg3.
    c = BaseHook.get_connection(POSTGRES_CONN_ID)
    conn = psycopg2.connect(host=c.host, port=c.port or 5432, user=c.login, password=c.password, dbname=c.schema)
    db.use_schema(conn, schema)
    return closing(conn)


def _github_client() -> GitHubClient:
    token = os.environ.get("GITHUB_TOKEN", "")
    if token:
        mask_secret(token)  # Airflow replaces it with *** if it ever appears in a log line
    return GitHubClient(token=token or None)


def _interval_start(ctx):
    return ctx.get("data_interval_start") or ctx.get("logical_date") or pendulum.now("UTC")


def _snapshot_date(ctx) -> str:
    return ctx["params"].get("snapshot_date") or _interval_start(ctx).in_timezone("UTC").to_date_string()


def _run_path():
    ctx = get_current_context()
    return storage.run_dir(ctx["dag"].dag_id, ctx["run_id"])


def _failed_tasks(ctx) -> list[str]:
    run_id, dag_obj = ctx["run_id"], ctx["dag"]
    states = RuntimeTaskInstance.get_task_states(dag_id=dag_obj.dag_id, run_ids=[run_id]).get(run_id, {})
    failed = set()
    for key, state in states.items():
        if state != "failed":  # root causes only; downstream tasks are then "upstream_failed"
            continue
        task_id = key if key in dag_obj.task_ids else key.rsplit("_", 1)[0]  # mapped tasks: "<task_id>_<index>"
        if task_id != ctx["ti"].task_id:
            failed.add(task_id)
    return sorted(failed)


def build_dag(dag_id: str, schema: str, source_mode: str, start_date, end_date=None, catchup: bool = False,
              retry_delay: timedelta = timedelta(minutes=1), sensor_timeout: timedelta = timedelta(hours=1)):
    params = {
        "orgs": Param(DEFAULT_ORGS, type="array", description="GitHub organisations to ingest"),
        "full_refresh": Param(False, type="boolean", description="Fetch every repo (and soft-delete missing ones)"),
        "simulate_failure": Param("none", enum=["none", "transient", "permanent"],
                                  description="transient: first extract attempt fails then recovers on retry; "
                                              "permanent: validation fails on every attempt"),
    }
    if source_mode == "fixture":
        params["snapshot_date"] = Param(None, type=["null", "string"], format="date",
                                        description="Replay this snapshot instead of the data interval's date")
    default_args = {
        "retries": 3,
        "retry_delay": retry_delay,
        "retry_exponential_backoff": True,  # 1x, 2x, 4x the delay (with jitter), capped below
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=20),
    }

    @dag(
        dag_id=dag_id,
        schedule=DAILY_UTC,
        start_date=start_date,
        end_date=end_date,
        catchup=catchup,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=3),
        default_args=default_args,
        params=params,
        tags=["github", "ingestion", source_mode],
        doc_md=__doc__,
    )
    def github_ingestion():
        @task
        def start_run() -> list[str]:
            ctx = get_current_context()
            orgs = list(ctx["params"]["orgs"])
            with _connect(schema) as conn:
                db.start_run(conn, ctx["run_id"], dag_id, source_mode, orgs, ctx.get("logical_date"),
                             ctx.get("data_interval_start"), ctx.get("data_interval_end"))
            log.info("Run %s started for orgs %s, interval %s -> %s", ctx["run_id"], orgs,
                     ctx.get("data_interval_start"), ctx.get("data_interval_end"))
            return orgs

        @task
        def plan_extraction(orgs: list[str]) -> list[dict]:
            ctx = get_current_context()
            with _connect(schema) as conn:
                checkpoints = db.get_checkpoints(conn, orgs)
            plan = pipeline.plan_extraction(orgs, checkpoints, _interval_start(ctx), ctx["params"]["full_refresh"])
            log.info("Extraction plan (None = full refresh): %s", plan)
            return [{"org": org, "since": since} for org, since in plan.items()]

        @task.sensor(task_id="check_api_quota", poke_interval=300, timeout=sensor_timeout.total_seconds(),
                     mode="reschedule", retries=1)
        def check_api_quota() -> PokeReturnValue:
            """Wait until GitHub has enough quota left for a full run (quota resets hourly)."""
            rl = _github_client().get_rate_limit()
            log.info("GitHub quota: %d/%d remaining, resets %s", rl.remaining, rl.limit, rl.reset_at)
            return PokeReturnValue(is_done=rl.remaining >= MIN_API_QUOTA,
                                   xcom_value={"remaining": rl.remaining, "reset_at": rl.reset_at.isoformat()})

        @task.sensor(task_id="wait_for_snapshot", poke_interval=30, timeout=sensor_timeout.total_seconds(),
                     mode="reschedule", retries=0)
        def wait_for_snapshot(orgs: list[str]) -> PokeReturnValue:
            """Wait until the day's snapshot file exists for every org (like waiting for an upstream export)."""
            snapshot = _snapshot_date(get_current_context())
            missing = missing_snapshots(FIXTURES_DIR, snapshot, orgs)
            log.info("Snapshot %s: missing orgs %s", snapshot, missing or "none")
            return PokeReturnValue(is_done=not missing, xcom_value=snapshot)

        @task(map_index_template="{{ job.org }}")
        def extract_repositories(job: dict) -> dict:
            ctx = get_current_context()
            if ctx["params"]["simulate_failure"] == "transient" and ctx["ti"].try_number == 1 \
                    and job["org"] == ctx["params"]["orgs"][0]:
                raise GitHubAPIError("Simulated failure: GitHub returned 503 Service Unavailable (attempt 1)")
            source = FixtureSource(FIXTURES_DIR, _snapshot_date(ctx)) if source_mode == "fixture" else _github_client()
            try:
                return pipeline.extract_org(source, job["org"], _run_path(), since=job["since"])
            except RateLimitExceeded as exc:
                # Not retried inside the client; Airflow's retry (with backoff) tries again later.
                raise AirflowException(f"{exc}. Will retry after the configured delay.") from exc

        @task(map_index_template="{{ manifest.org }}")
        def validate_repositories(manifest: dict) -> dict:
            ctx = get_current_context()
            if ctx["params"]["simulate_failure"] == "permanent" and manifest["org"] == ctx["params"]["orgs"][0]:
                raise ValueError(f"Simulated failure: unreadable raw data for {manifest['org']} (fails every attempt)")
            return pipeline.validate_org(manifest, _run_path())

        @task
        def load_and_quarantine(summaries: list[dict]) -> dict:
            with _connect(schema) as conn:
                return pipeline.load_results(conn, get_current_context()["run_id"], list(summaries))

        @task(trigger_rule="all_done")
        def finalize_run() -> dict:
            """Always runs; records the true outcome and fails the DAG run if anything upstream failed."""
            ctx = get_current_context()
            failed = _failed_tasks(ctx)
            counts = ctx["ti"].xcom_pull(task_ids="load_and_quarantine") or {}
            status = "failed" if failed else "success"
            with _connect(schema) as conn:
                db.ensure_run(conn, ctx["run_id"], dag_id, source_mode, list(ctx["params"]["orgs"]))
                db.finish_run(conn, ctx["run_id"], status, counts, failed_tasks=failed,
                              error_message=f"Failed tasks: {', '.join(failed)}. See task logs." if failed else None)
            log.info("Run %s finished with status=%s counts=%s", ctx["run_id"], status, counts)
            if failed:
                raise AirflowFailException(f"Pipeline failed in: {', '.join(failed)}")
            return {"status": status, **counts}

        orgs = start_run()
        plan = plan_extraction(orgs)
        manifests = extract_repositories.expand(job=plan)
        ready = check_api_quota() if source_mode == "live" else wait_for_snapshot(orgs)
        plan >> ready >> manifests
        summaries = validate_repositories.expand(manifest=manifests)
        load_and_quarantine(summaries) >> finalize_run()

    return github_ingestion()


github_ingestion = build_dag(
    "github_ingestion", schema="public", source_mode="live",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
)
github_ingestion_fixture = build_dag(
    "github_ingestion_fixture", schema="fixture", source_mode="fixture",
    start_date=pendulum.datetime(2026, 11, 18, tz="UTC"), end_date=pendulum.datetime(2026, 11, 24, tz="UTC"),
    catchup=True, retry_delay=timedelta(seconds=5), sensor_timeout=timedelta(minutes=2),
)

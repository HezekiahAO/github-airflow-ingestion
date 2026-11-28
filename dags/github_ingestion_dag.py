"""GitHub repository metadata ingestion.

Two DAGs share the same tasks and code path:
  github_ingestion          live GitHub API, schema `public`
  github_ingestion_fixture  replayed daily snapshots, schema `fixture` (reproducible backfills)

start_run -> plan_extraction -> [check_api_quota] -> extract_repositories[org]
          -> validate_repositories[org] -> load_and_quarantine -> finalize_run

Big data (API responses, validated records) is written to files on the shared
./data volume; XCom only carries file paths, timestamps and counts.

Scheduling: daily at 00:00 UTC with CronDataIntervalTimetable, so each run covers
the interval [data_interval_start, data_interval_end) and starts after it ends.
The live API only knows *current* state, so the live DAG does not catch up;
historical intervals are replayed with the fixture DAG instead.
"""
import logging
import os
from contextlib import closing

import pendulum
import psycopg2
from airflow.sdk import BaseHook, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.sdk.log import mask_secret
from airflow.timetables.interval import CronDataIntervalTimetable

from github_ingestion import db, pipeline, storage
from github_ingestion.fixtures import FixtureSource
from github_ingestion.github_client import GitHubClient

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
    return _interval_start(ctx).in_timezone("UTC").to_date_string()


def _run_path():
    ctx = get_current_context()
    return storage.run_dir(ctx["dag"].dag_id, ctx["run_id"])


def _failed_tasks(ctx) -> list[str]:
    run_id, dag_obj = ctx["run_id"], ctx["dag"]
    states = RuntimeTaskInstance.get_task_states(dag_id=dag_obj.dag_id, run_ids=[run_id]).get(run_id, {})
    failed = set()
    for key, state in states.items():
        if state not in ("failed", "upstream_failed"):
            continue
        task_id = key if key in dag_obj.task_ids else key.rsplit("_", 1)[0]  # mapped tasks: "<task_id>_<index>"
        if task_id != ctx["ti"].task_id:
            failed.add(task_id)
    return sorted(failed)


def build_dag(dag_id: str, schema: str, source_mode: str, start_date, end_date=None, catchup: bool = False):
    params = {
        "orgs": Param(DEFAULT_ORGS, type="array", description="GitHub organisations to ingest"),
        "full_refresh": Param(False, type="boolean", description="Fetch every repo (and soft-delete missing ones)"),
    }

    @dag(
        dag_id=dag_id,
        schedule=DAILY_UTC,
        start_date=start_date,
        end_date=end_date,
        catchup=catchup,
        max_active_runs=1,
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

        @task
        def check_api_quota() -> dict:
            rl = _github_client().get_rate_limit()
            log.info("GitHub quota: %d/%d remaining, resets %s", rl.remaining, rl.limit, rl.reset_at)
            if rl.remaining < MIN_API_QUOTA:
                raise AirflowFailException(f"Only {rl.remaining} API calls left (need {MIN_API_QUOTA}); resets {rl.reset_at}")
            return {"remaining": rl.remaining, "reset_at": rl.reset_at.isoformat()}

        @task(map_index_template="{{ job.org }}")
        def extract_repositories(job: dict) -> dict:
            ctx = get_current_context()
            if source_mode == "fixture":
                source = FixtureSource(FIXTURES_DIR, _snapshot_date(ctx))
            else:
                source = _github_client()
            return pipeline.extract_org(source, job["org"], _run_path(), since=job["since"])

        @task(map_index_template="{{ manifest.org }}")
        def validate_repositories(manifest: dict) -> dict:
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
        if source_mode == "live":
            plan >> check_api_quota() >> manifests
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
    catchup=True,
)

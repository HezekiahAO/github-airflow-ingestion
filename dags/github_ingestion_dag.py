"""GitHub repository metadata ingestion.

start_run -> check_api_quota -> extract_repositories[org] -> validate_repositories[org]
          -> load_and_quarantine -> finalize_run

Big data (API responses, validated records) is written to files on the shared
./data volume; XCom only carries file paths and counts.
"""
import logging
import os
from contextlib import closing

import pendulum
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.sdk.log import mask_secret

from github_ingestion import db, pipeline, storage
from github_ingestion.github_client import GitHubClient

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "github_postgres"
DEFAULT_ORGS = [o.strip() for o in os.environ.get("GITHUB_ORGS", "stripe,shopify,microsoft").split(",") if o.strip()]
MIN_API_QUOTA = int(os.environ.get("GITHUB_MIN_QUOTA", "200"))


def _connect(schema: str):
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    db.use_schema(conn, schema)
    return closing(conn)


def _github_client() -> GitHubClient:
    token = os.environ.get("GITHUB_TOKEN", "")
    if token:
        mask_secret(token)  # Airflow replaces it with *** if it ever appears in a log line
    return GitHubClient(token=token or None)


def _run_path():
    ctx = get_current_context()
    return storage.run_dir(ctx["dag"].dag_id, ctx["run_id"])


def build_dag(dag_id: str, schema: str, source_mode: str):
    @dag(
        dag_id=dag_id,
        schedule=None,
        start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        params={"orgs": Param(DEFAULT_ORGS, type="array", description="GitHub organisations to ingest")},
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
            log.info("Run %s started for orgs %s", ctx["run_id"], orgs)
            return orgs

        @task
        def check_api_quota() -> dict:
            rl = _github_client().get_rate_limit()
            log.info("GitHub quota: %d/%d remaining, resets %s", rl.remaining, rl.limit, rl.reset_at)
            if rl.remaining < MIN_API_QUOTA:
                raise AirflowFailException(f"Only {rl.remaining} API calls left (need {MIN_API_QUOTA}); resets {rl.reset_at}")
            return {"remaining": rl.remaining, "reset_at": rl.reset_at.isoformat()}

        @task
        def extract_repositories(org: str) -> dict:
            return pipeline.extract_org(_github_client(), org, _run_path())

        @task
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
            run_id, ti = ctx["run_id"], ctx["ti"]
            states = RuntimeTaskInstance.get_task_states(dag_id=dag_id, run_ids=[run_id]).get(run_id, {})
            failed = sorted({
                tid.rsplit("_", 1)[0] if tid.rsplit("_", 1)[-1].isdigit() and tid not in ctx["dag"].task_ids else tid
                for tid, state in states.items()
                if state in ("failed", "upstream_failed") and tid != ti.task_id
            })
            counts = ti.xcom_pull(task_ids="load_and_quarantine") or {}
            status = "failed" if failed else "success"
            with _connect(schema) as conn:
                db.ensure_run(conn, run_id, dag_id, source_mode, list(ctx["params"]["orgs"]))
                db.finish_run(conn, run_id, status, counts, failed_tasks=failed,
                              error_message=f"Failed tasks: {', '.join(failed)}. See task logs." if failed else None)
            log.info("Run %s finished with status=%s counts=%s", run_id, status, counts)
            if failed:
                raise AirflowFailException(f"Pipeline failed in: {', '.join(failed)}")
            return {"status": status, **counts}

        orgs = start_run()
        quota = check_api_quota()
        manifests = extract_repositories.expand(org=orgs)
        summaries = validate_repositories.expand(manifest=manifests)
        loaded = load_and_quarantine(summaries)
        orgs >> quota >> manifests
        loaded >> finalize_run()

    return github_ingestion()


github_ingestion = build_dag("github_ingestion", schema="public", source_mode="live")

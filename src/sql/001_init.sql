-- Idempotent schema: So this is safe to run on every deploy (CREATE ... IF NOT EXISTS).
-- Tables are created in whatever schema is first on search_path.

-- One row per Airflow DAG run: the audit trail of what happened.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              TEXT PRIMARY KEY,               -- Airflow dag_run.run_id
    dag_id              TEXT NOT NULL,
    logical_date        TIMESTAMPTZ,
    data_interval_start TIMESTAMPTZ,
    data_interval_end   TIMESTAMPTZ,
    source_mode         TEXT NOT NULL CHECK (source_mode IN ('live', 'fixture')),
    orgs                TEXT[] NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at            TIMESTAMPTZ,
    pages_fetched       INTEGER CHECK (pages_fetched >= 0),
    records_extracted   INTEGER CHECK (records_extracted >= 0),
    records_valid       INTEGER CHECK (records_valid >= 0),
    records_invalid     INTEGER CHECK (records_invalid >= 0),
    records_inserted    INTEGER CHECK (records_inserted >= 0),
    records_updated     INTEGER CHECK (records_updated >= 0),
    records_unchanged   INTEGER CHECK (records_unchanged >= 0),
    records_quarantined INTEGER CHECK (records_quarantined >= 0),
    quality_report      JSONB,
    failed_tasks        TEXT[],
    error_message       TEXT,
    CHECK (ended_at IS NULL OR ended_at >= started_at),
    CHECK (status = 'running' OR ended_at IS NOT NULL)
);

-- Current state of every repository. One row per GitHub repo id.
CREATE TABLE IF NOT EXISTS github_repos (
    repo_id             BIGINT PRIMARY KEY CHECK (repo_id > 0),
    org                 TEXT NOT NULL,
    name                TEXT NOT NULL,
    full_name           TEXT NOT NULL,
    html_url            TEXT NOT NULL CHECK (html_url LIKE 'https://github.com/%'),
    description         TEXT,
    language            TEXT,
    license             TEXT,
    default_branch      TEXT NOT NULL,
    visibility          TEXT NOT NULL,
    is_fork             BOOLEAN NOT NULL,
    is_archived         BOOLEAN NOT NULL,
    stargazers_count    INTEGER NOT NULL CHECK (stargazers_count >= 0),
    forks_count         INTEGER NOT NULL CHECK (forks_count >= 0),
    open_issues_count   INTEGER NOT NULL CHECK (open_issues_count >= 0),
    watchers_count      INTEGER NOT NULL CHECK (watchers_count >= 0),
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    pushed_at           TIMESTAMPTZ,
    is_deleted          BOOLEAN NOT NULL DEFAULT false,   -- missing from a full refresh
    deleted_detected_at TIMESTAMPTZ,
    -- audit fields
    record_hash         TEXT NOT NULL,                    -- detects real changes vs re-sends
    first_seen_run_id   TEXT NOT NULL REFERENCES pipeline_runs (run_id),
    last_seen_run_id    TEXT NOT NULL REFERENCES pipeline_runs (run_id),
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (updated_at >= created_at)
);
CREATE INDEX IF NOT EXISTS idx_github_repos_org_updated ON github_repos (org, updated_at DESC);

-- Records that failed validation, kept with their raw payload for debugging.
-- payload_hash makes it idempotent: the same bad payload is stored once and counted.
CREATE TABLE IF NOT EXISTS github_repos_quarantine (
    quarantine_id       BIGSERIAL PRIMARY KEY,
    payload_hash        TEXT NOT NULL UNIQUE,
    repo_id             BIGINT,
    org                 TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    validation_errors   JSONB NOT NULL,
    source_file         TEXT,
    first_seen_run_id   TEXT NOT NULL REFERENCES pipeline_runs (run_id),
    last_seen_run_id    TEXT NOT NULL REFERENCES pipeline_runs (run_id),
    times_seen          INTEGER NOT NULL DEFAULT 1 CHECK (times_seen >= 1),
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_quarantine_repo ON github_repos_quarantine (repo_id);

-- High-water mark per organisation for incremental extraction.
CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
    org                 TEXT PRIMARY KEY,
    last_updated_at     TIMESTAMPTZ NOT NULL,   -- max(updated_at) successfully loaded
    last_run_id         TEXT NOT NULL REFERENCES pipeline_runs (run_id),
    checkpointed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per quality metric per run.
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    run_id              TEXT NOT NULL REFERENCES pipeline_runs (run_id),
    metric              TEXT NOT NULL CHECK (metric IN ('completeness', 'validity', 'uniqueness', 'consistency', 'accuracy')),
    value               NUMERIC(6, 5) NOT NULL CHECK (value BETWEEN 0 AND 1),
    numerator           INTEGER NOT NULL,
    denominator         INTEGER NOT NULL,
    threshold           NUMERIC(6, 5) NOT NULL,
    severity            TEXT NOT NULL CHECK (severity IN ('warn', 'error')),
    passed              BOOLEAN NOT NULL,
    definition          TEXT NOT NULL,
    measured_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, metric)
);

import pytest


@pytest.fixture
def raw_repo() -> dict:
    """A trimmed copy of a real GitHub /orgs/{org}/repos item."""
    return {
        "id": 123,
        "name": "stripe-python",
        "full_name": "stripe/stripe-python",
        "owner": {"login": "stripe", "id": 856813},
        "html_url": "https://github.com/stripe/stripe-python",
        "description": "Python library for the Stripe API.",
        "fork": False,
        "archived": False,
        "language": "Python",
        "license": {"key": "mit", "spdx_id": "MIT"},
        "default_branch": "master",
        "visibility": "public",
        "stargazers_count": 1700,
        "forks_count": 400,
        "open_issues_count": 20,
        "watchers_count": 1700,
        "created_at": "2011-04-14T00:00:00Z",
        "updated_at": "2026-11-20T10:00:00Z",
        "pushed_at": "2026-11-20T09:00:00Z",
        "node_id": "ignored-extra-field",
    }


import os
import uuid

DB_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.fixture
def pg_conn():
    """A connection to a fresh, throwaway schema with all tables migrated.

    Set TEST_DATABASE_URL (e.g. postgresql://postgres:test@localhost:5433/postgres)
    to run these tests; they are skipped otherwise.
    """
    if not DB_URL:
        pytest.skip("TEST_DATABASE_URL not set")
    psycopg2 = pytest.importorskip("psycopg2")
    from github_ingestion import db

    schema = f"test_{uuid.uuid4().hex[:10]}"
    conn = psycopg2.connect(DB_URL)
    db.migrate(conn, schema)
    try:
        yield conn
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
        conn.commit()
        conn.close()

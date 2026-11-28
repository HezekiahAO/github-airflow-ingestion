"""Create/upgrade the database tables.

    python -m github_ingestion.migrate                # schemas: public fixture
    python -m github_ingestion.migrate public

The connection string is read from AIRFLOW_CONN_GITHUB_POSTGRES (the same
variable Airflow uses for the `github_postgres` connection) or GITHUB_DB_URL.
"""
import os
import sys

import psycopg2

from github_ingestion import db


def main(argv: list[str]) -> None:
    dsn = os.environ.get("AIRFLOW_CONN_GITHUB_POSTGRES") or os.environ.get("GITHUB_DB_URL")
    if not dsn:
        sys.exit("Set AIRFLOW_CONN_GITHUB_POSTGRES or GITHUB_DB_URL")
    schemas = argv or ["public", "fixture"]
    with psycopg2.connect(dsn) as conn:
        for schema in schemas:
            applied = db.migrate(conn, schema)
            print(f"schema '{schema}': applied {', '.join(applied)}")


if __name__ == "__main__":
    main(sys.argv[1:])

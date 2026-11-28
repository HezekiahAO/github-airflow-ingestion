#!/bin/bash
# Runs once, the first time the Postgres volume is created.
# Creates a separate database (and a test database) for pipeline data so it
# never mixes with Airflow's own metadata database.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER "${GITHUB_DB_USER}" WITH PASSWORD '${GITHUB_DB_PASSWORD}';
    CREATE DATABASE "${GITHUB_DB_NAME}" OWNER "${GITHUB_DB_USER}";
    CREATE DATABASE "${GITHUB_DB_NAME}_test" OWNER "${GITHUB_DB_USER}";
EOSQL

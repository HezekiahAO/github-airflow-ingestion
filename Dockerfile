FROM apache/airflow:3.3.2-python3.12

# pydantic, requests, psycopg2 and the Postgres provider already ship with this
# image; we only add the test tools so `pytest` can run inside the container.
RUN pip install --no-cache-dir "pytest>=8,<10" "responses>=0.25,<1"

ENV PYTHONPATH=/opt/airflow/src

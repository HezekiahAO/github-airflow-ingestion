# Evidence (fixture schema, 7-day controlled backfill)

Screenshots are in `docs/screenshots/`:
| File | Shows |
|---|---|
| `01_dags_list.png` | both DAGs imported, no import errors |
| `02_fixture_dag_grid.png` | grid of the 7 backfill runs, all green |
| `03_fixture_dag_graph.png` | one successful backfill run, every task succeeded |
| `04_transient_failure_recovered.png` | `simulate_failure=transient`: extract for stripe failed on try 1, succeeded on try 2, run succeeded |
| `05_permanent_failure.png` | `simulate_failure=permanent`: validate failed after 4 tries, downstream `upstream_failed`, finalize recorded failure, run failed |
| `06_sensor_timeout.png` | missing snapshot: `wait_for_snapshot` timed out, run recorded as failed |
| `07_backfill_runs.png` | run list |
| `08_live_dag_graph.png` | live DAG structure |

## First backfill (empty schema, 2026-11-18 .. 2026-11-24, `--max-active-runs 1`)
```
 logical   | status  | ex | ins | upd | unch | q | quality warnings
-----------+---------+----+-----+-----+------+---+----------------
 11-18 Wed | success | 21 |  20 |   0 |    0 | 1 | []
 11-19 Thu | success | 18 |   0 |   3 |   14 | 0 | ["uniqueness"]
 11-20 Fri | success | 18 |   1 |   3 |   14 | 0 | []
 11-21 Sat | success | 18 |   0 |   4 |   14 | 0 | []
 11-22 Sun | success | 18 |   0 |   3 |   14 | 1 | []
 11-23 Mon | success | 22 |   0 |   3 |   17 | 2 | []
 11-24 Tue | success | 18 |   0 |   2 |   15 | 1 | []
```
11-18 is the first load (full). 11-19 contains a duplicate repo id (uniqueness warning). 11-20 adds a new repo.
11-23 is a Monday, so it runs a full refresh: it extracts more records and soft-deletes the Microsoft repo that disappeared.

## Second backfill of the same dates (repeat-run check)
Every run: 0 inserts, 0 updates. Repeating the runs created no duplicates and changed nothing.
Run ids are named after the end of the interval (Airflow 3 naming), so logical date 11-18 = `scheduled__2026-11-19...`.
```
                run_id                | status  | ex | ins | upd | unch | q | failed_tasks 
--------------------------------------+---------+----+-----+-----+------+---+--------------
 scheduled__2026-11-19T00:00:00+00:00 | success |  9 |   0 |   0 |    2 | 0 | 
 scheduled__2026-11-20T00:00:00+00:00 | success |  9 |   0 |   0 |    5 | 0 | 
 scheduled__2026-11-21T00:00:00+00:00 | success |  9 |   0 |   0 |    8 | 0 | 
 scheduled__2026-11-22T00:00:00+00:00 | success |  9 |   0 |   0 |    8 | 0 | 
 scheduled__2026-11-23T00:00:00+00:00 | success |  9 |   0 |   0 |    7 | 1 | 
 scheduled__2026-11-24T00:00:00+00:00 | success | 22 |   0 |   0 |   18 | 2 | 
 scheduled__2026-11-25T00:00:00+00:00 | success | 18 |   0 |   0 |   17 | 1 | 
(7 rows)

 repos | distinct_ids | soft_deleted 
-------+--------------+--------------
    21 |           21 |            1
(1 row)

    org    |        full_name         |                                           errors                                           | times_seen 
-----------+--------------------------+--------------------------------------------------------------------------------------------+------------
 microsoft | microsoft/something-else | [{"field": "__root__", "message": "Value error, full_name 'microsoft/something-else'}] does  |          1
 shopify   | shopify/broken-fixture   | [{"field": "forks_count", "message": "Input should be greater than or equal to 0"}]        |          2
 stripe    | stripe/no-url            | [{"field": "html_url", "message": "Field required"}]                                       |          1
 stripe    | stripe/time-travel       | [{"field": "__root__", "message": "Value error, updated_at is earlier than created_at"}]   |          1
(4 rows)

    org    |    last_updated_at     |             last_run_id              |        checkpointed_at        
-----------+------------------------+--------------------------------------+-------------------------------
 microsoft | 2026-11-24 12:00:00+00 | scheduled__2026-11-25T00:00:00+00:00 | 2026-11-27 13:31:31.961359+00
 shopify   | 2026-11-23 12:00:00+00 | scheduled__2026-11-25T00:00:00+00:00 | 2026-11-27 13:31:31.961359+00
 stripe    | 2026-11-24 12:00:00+00 | scheduled__2026-11-25T00:00:00+00:00 | 2026-11-27 13:31:31.961359+00
(3 rows)

    metric    | value  | threshold | severity | passed 
--------------+--------+-----------+----------+--------
 accuracy     | 1.0000 |   0.99000 | warn     | t
 completeness | 0.9921 |   0.99000 | error    | t
 consistency  | 1.0000 |   1.00000 | error    | t
 uniqueness   | 1.0000 |   1.00000 | warn     | t
 validity     | 0.9444 |   0.90000 | warn     | t
(5 rows)

```

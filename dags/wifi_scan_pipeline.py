from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from tasks.extract      import extract_bronze
from tasks.minio_upload import upload_to_minio

DBT_PROJECT_DIR = "/opt/airflow/dbt"

# ─────────────────────────────────────────────
# Default args apply to every task in the DAG.
#
# retries=1 → if a task fails, retry once
# retry_delay → wait 2 minutes before retrying
#
# Why retry? Network blips, Supabase timeouts,
# MinIO momentary unavailability. One retry
# handles transient failures without alerting.
# ─────────────────────────────────────────────
default_args = {
    "owner":       "espyguard",
    "retries":     1,
    "retry_delay": timedelta(minutes=2),
    "start_date":  datetime(2025, 1, 1),
}

with DAG(
    dag_id       = "wifi_scan_pipeline",
    description  = "Bronze extraction → MinIO archive + dbt Silver/Gold",
    schedule     = "*/30 * * * *",  # cron: every 30 minutes
    default_args = default_args,
    catchup      = False,           # don't backfill missed runs
    tags         = ["espyguard", "wifi", "bronze", "silver", "gold"]
) as dag:

    # ── Task 1 ────────────────────────────────
    # Reads Bronze from Supabase, pushes to XCom
    t_extract = PythonOperator(
        task_id         = "extract_bronze",
        python_callable = extract_bronze,
    )

    # ── Task 2a ───────────────────────────────
    # Pulls XCom data, archives to MinIO
    # Runs in PARALLEL with t_dbt_silver
    t_minio = PythonOperator(
        task_id         = "upload_to_minio",
        python_callable = upload_to_minio,
    )

    # ── Task 2b ───────────────────────────────
    # Runs dbt Silver models
    # Reads Bronze from Supabase directly
    # Runs in PARALLEL with t_minio
    t_dbt_silver = BashOperator(
        task_id      = "run_dbt_silver",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select silver "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        )
    )

    # ── Task 3 ────────────────────────────────
    # Runs dbt Gold models
    # Depends on Silver being complete
    # Waits for BOTH t_minio and t_dbt_silver
    t_dbt_gold = BashOperator(
        task_id      = "run_dbt_gold",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select gold "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        )
    )

    # ── Pipeline Flow ─────────────────────────
    # extract → [minio + silver in parallel] → gold
    t_extract >> [t_minio, t_dbt_silver] >> t_dbt_gold
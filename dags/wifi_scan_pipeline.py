from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from tasks.extract      import extract_bronze
from tasks.minio_upload import upload_to_minio

DBT_PROJECT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner":       "espyguard",
    "retries":     1,
    "retry_delay": timedelta(minutes=2),
    "start_date":  datetime(2025, 1, 1),
}

with DAG(
    dag_id       = "wifi_scan_pipeline",
    description  = "Bronze extraction → MinIO archive + dbt Silver/Gold",
    schedule     = "*/30 * * * *",
    default_args = default_args,
    catchup      = False,
    tags         = ["espyguard", "wifi", "bronze", "silver", "gold"]
) as dag:

    t_extract = PythonOperator(
        task_id         = "extract_bronze",
        python_callable = extract_bronze,
    )

    t_minio = PythonOperator(
        task_id         = "upload_to_minio",
        python_callable = upload_to_minio,
    )

    #  runs "dbt deps" before anything else
    t_dbt_deps = BashOperator(
        task_id      = "dbt_deps",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROJECT_DIR}"
        )
    )

    t_dbt_silver = BashOperator(
        task_id      = "run_dbt_silver",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select silver "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--full-refresh"
        )
    )

    t_dbt_gold = BashOperator(
        task_id      = "run_dbt_gold",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select gold "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--full-refresh"
        )
    )

    #  add t_dbt_deps at the start of the chain
    t_extract >> t_dbt_deps >> [t_minio, t_dbt_silver] >> t_dbt_gold
"""Airflow DAG：调度纯 Python ETL。"""

# pyright: reportMissingImports=false, reportAttributeAccessIssue=false

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="python_etl_pipeline",
    start_date=datetime(2026, 4, 16),
    schedule="@daily",
    catchup=False,
    tags=["etl", "python", "learning"],
) as dag:
    run_python_etl = BashOperator(
        task_id="run_python_etl",
        bash_command='python -m etl_project --target "${ETL_TARGET:-duckdb}" --source "${ETL_DATA_SOURCE:-csv}"',
        cwd=os.environ.get("ETL_PROJECT_ROOT", "."),
        env={
            "ETL_TARGET": os.environ.get("ETL_TARGET", "duckdb"),
            "ETL_DATA_SOURCE": os.environ.get("ETL_DATA_SOURCE", "csv"),
            "ETL_POSTGRES_DSN": os.environ.get("ETL_POSTGRES_DSN", ""),
        },
    )

    run_python_etl

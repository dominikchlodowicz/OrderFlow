import os
from datetime import timedelta

import pendulum
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)
from airflow.sdk import DAG


ORDERFLOW_JOB_ID = int(os.environ["ORDERFLOW_JOB_ID"])


with DAG(
    dag_id="orderflow_pipeline",
    description="Orchestrates the OrderFlow Lakeflow job",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=[
        "orderflow",
        "databricks",
        "dev",
    ],
) as dag:
    run_lakeflow_job = DatabricksRunNowOperator(
        task_id="run_lakeflow_job",
        databricks_conn_id="databricks_default",
        job_id=ORDERFLOW_JOB_ID,
        json={
            "job_parameters": {
                "batch_id": "{{ dag_run.conf.get('batch_id', ds) }}",
                "landing_path": (
                    "abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/"
                    "bronze/landing"
                ),
                "orchestrator": "airflow",
                "orchestrator_run_id": "{{ run_id }}",
            }
        },
        wait_for_termination=True,
        deferrable=True,
        polling_period_seconds=30,
        execution_timeout=timedelta(hours=3),
    )
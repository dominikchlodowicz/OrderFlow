"""Integrity checks for the OrderFlow orchestration DAG."""

from airflow.models import DagBag


def test_orderflow_dag_imports_without_errors() -> None:
    dag_bag = DagBag(include_examples=False)

    assert dag_bag.import_errors == {}
    assert "orderflow_pipeline" in dag_bag.dags


def test_orderflow_dag_triggers_the_complete_lakeflow_job() -> None:
    dag = DagBag(include_examples=False).dags.get("orderflow_pipeline")

    assert dag is not None
    assert {task.task_id for task in dag.tasks} == {"run_lakeflow_job"}

    run_lakeflow_job = dag.get_task("run_lakeflow_job")
    assert isinstance(run_lakeflow_job.job_id, int)
    assert run_lakeflow_job.job_id > 0
    assert run_lakeflow_job.databricks_conn_id == "databricks_default"
    assert run_lakeflow_job.json["job_parameters"]["orchestrator"] == "airflow"
    assert run_lakeflow_job.wait_for_termination is True

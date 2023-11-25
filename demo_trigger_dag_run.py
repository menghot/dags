from datetime import datetime

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import models

with models.DAG(
        dag_id="demo-trigger-dag",
        schedule="* * * * *",  # Override to match your needs
        start_date=datetime(2023, 11, 24),
        tags=["demo"],
        catchup=False,
) as dag:
    example_trigger = TriggerDagRunOperator(
        task_id="example_trigger",
        trigger_dag_id="example_callback",
        conf={"notice": "Hello DAG!"}
    )

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
        task_id="trigger_spark",
        trigger_dag_id="demo_spark_submit",
        conf={"notice": "Hello DAG!"}
    )

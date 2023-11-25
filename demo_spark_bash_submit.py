import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from operators.spark_submit import SparkBashSubmitOperator


def task_failure_alert(context):
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


def dag_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}, execution_date: {context['execution_date']}")


with DAG(
        dag_id="demo_spark_submit",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        dagrun_timeout=datetime.timedelta(minutes=60),
        catchup=False,
        on_success_callback=None,
        on_failure_callback=task_failure_alert,
        tags=["example"],
):
    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    task3 = BashOperator(task_id="task3", bash_command="echo example_callback 123",
                         on_success_callback=[dag_success_alert])
    task4 = SparkBashSubmitOperator(
        task_id="task4",
        bash_command="echo task4",
    )

    task5 = SparkBashSubmitOperator(
        task_id="task5",
        bash_command="echo sb5",
    )

    task1 >> task2 >> task3 >> task4 >> task5

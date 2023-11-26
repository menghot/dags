import datetime

import pendulum
from airflow import DAG

from operators.spark_submit import SparkBashSubmitOperator


def task_failure_alert(context):
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


def dag_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}, execution_date: {context['execution_date']}")


with DAG(
        dag_id="demo_spark_rest_submit",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        dagrun_timeout=datetime.timedelta(minutes=60),
        catchup=False,
        on_success_callback=None,
        on_failure_callback=task_failure_alert,
        tags=["example"],
):
    task1 = SparkBashSubmitOperator(
        task_id="task1",
        bash_command="spark-submit --master spark://10.194.183.226:6066 "
                     "--deploy-mode cluster "
                     "--num-executors 2 "
                     "--executor-cores 2 "
                     "--conf spark.master.rest.enabled=true "
                     "--conf spark.master.rest.enabled=true "
                     "--class org.apache.spark.examples.SparkPi "
                     "/opt/spark-3.4.1-bin-hadoop3/examples/jars/spark-examples_2.12-3.4.1.jar",
    )

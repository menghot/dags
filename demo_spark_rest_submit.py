import datetime

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
                     "--class org.apache.spark.examples.SparkPi "
                     "/opt/spark-3.4.1-bin-hadoop3/examples/jars/spark-examples_2.12-3.4.1.jar",
    )

    task2 = SparkSubmitOperator(
        task_id='task2',
        conn_id='spark_rest',
        java_class='org.example.JavaSparkPi',
        application='hdfs://10.194.186.216:8020/tmp/demo-spark-iceberg-1.0-SNAPSHOT.jar',
        total_executor_cores=2,  # Number of cores for the job
        executor_cores=2,  # Number of cores per executor
        executor_memory='2g',  # Memory per executor
        name='spark-pi-job',  # Name of the job
        verbose=True,
        conf={"spark.master.rest.enabled": "true"},
    )

from datetime import datetime

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import models
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from operators.spark_submit import SparkBashSubmitOperator

with models.DAG(
        dag_id="demo-trigger-dag",
        # schedule="*/5 * * * *",  # Override to match your needs
        schedule=None,  # Override to match your needs
        start_date=datetime(2023, 11, 24),
        tags=["demo"],
        catchup=False,
) as dag:

    example_trigger = TriggerDagRunOperator(
        task_id="trigger_spark",
        trigger_dag_id="demo_spark_submit",
        conf={"notice": "Hello DAG!"}
    )

    submit_spark_job = SparkBashSubmitOperator(
        task_id="submit_spark_job",
        bash_command="spark-submit "
                     "--master k8s://https://10.194.183.222:6443 "
                     "--deploy-mode cluster "
                     "--name spark-pi "
                     "--class org.apache.spark.examples.SparkPi "
                     "--conf spark.executor.instances=2 "
                     "--conf spark.kubernetes.node.selector.kubernetes.io/hostname=node-10-194-183-226 "
                     "--conf spark.kubernetes.container.image=apache/spark:3.5.0 "
                     "--conf spark.kubernetes.submission.waitAppCompletion=true "
                     "--conf spark.kubernetes.driver.request.cores=1 "
                     "--conf spark.kubernetes.driver.limit.cores=1 "
                     "--conf spark.kubernetes.executor.request.cores=1 "
                     "--conf spark.kubernetes.executor.limit.cores=2 "
                     "--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark "
                     "--conf spark.kubernetes.namespace=dtp "
                     "--conf spark.kubernetes.driver.volumes.hostPath.logs-dir.mount.path=/opt/spark/logs "
                     "--conf spark.kubernetes.driver.volumes.hostPath.logs-dir.options.path=/opt/spark/logs "
                     "--conf spark.kubernetes.executor.volumes.hostPath.logs-dir.mount.path=/opt/spark/logs "
                     "--conf spark.kubernetes.executor.volumes.hostPath.logs-dir.options.path=/opt/spark/logs "
                     "--conf spark.eventLog.enabled=true "
                     "--conf spark.eventLog.dir=file:///opt/spark/logs "
                     "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar ",
    )

    spark_operator_submit = SparkSubmitOperator(
        task_id='spark_operator_submit',
        conn_id='spark_k8s',
        java_class='org.example.JavaSparkPi',
        application='hdfs://10.194.186.216:8020/tmp/demo-spark-iceberg-1.0-SNAPSHOT.jar',
        application_args=["5"],
        total_executor_cores=2,  # Number of cores for the job
        executor_cores=1,  # Number of cores per executor
        executor_memory='1g',  # Memory per executor
        name='spark-k8s',
        verbose=True,
        conf={
            "spark.kubernetes.node.selector.kubernetes.io/hostname": "node-10-194-183-226",
            "spark.kubernetes.container.image": "apache/spark:3.5.0",
            "spark.kubernetes.submission.waitAppCompletion": "true",
            "spark.kubernetes.driver.request.cores": "1",
            "spark.kubernetes.driver.limit.cores": "1",
            "spark.kubernetes.executor.request.cores": "1",
            "spark.kubernetes.executor.limit.cores": "1",
            "spark.kubernetes.authenticate.driver.serviceAccountNam": "spark",
            "spark.kubernetes.driver.volumes.hostPath.logs-dir.mount.path": "/opt/spark/logs",
            "spark.kubernetes.driver.volumes.hostPath.logs-dir.options.path": "/opt/spark/logs",
            "spark.kubernetes.executor.volumes.hostPath.logs-dir.mount.path": "/opt/spark/logs",
            "spark.kubernetes.executor.volumes.hostPath.logs-dir.options.path": "/opt/spark/logs",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "file:///opt/spark/logs",
            "spark.kubernetes.namespace": "dtp",
        },
        dag=dag
    )

    example_trigger >> submit_spark_job >> spark_operator_submit

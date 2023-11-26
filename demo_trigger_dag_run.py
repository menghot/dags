from datetime import datetime

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import models

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

    # spark-submit --master k8s://https://10.194.183.222:6443 --deploy-mode cluster --name spark-pi --class
    # org.apache.spark.examples.SparkPi --conf spark.executor.instances=2 --conf
    # spark.kubernetes.node.selector.kubernetes.io/hostname=node-10-194-183-226 --conf
    # spark.kubernetes.container.image=apache/spark:3.5.0 --conf spark.kubernetes.submission.waitAppCompletion=true
    # --conf spark.kubernetes.driver.request.cores=1 --conf spark.kubernetes.driver.limit.cores=1 --conf
    # spark.driver.memory=2g --conf spark.executor.memory=2g --conf spark.kubernetes.executor.request.cores=1 --conf
    # spark.kubernetes.executor.limit.cores=1 --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark
    # --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.vol1.options.claimName=spark-data --conf
    # spark.kubernetes.driver.volumes.persistentVolumeClaim.vol1.mount.path=/opt/spark/logs --conf
    # spark.kubernetes.executor.volumes.persistentVolumeClaim.vol1.options.claimName=spark-data --conf
    # spark.kubernetes.executor.volumes.persistentVolumeClaim.vol1.mount.path=/opt/spark/logs --conf
    # spark.kubernetes.namespace=dtp local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar

    # --conf spark.kubernetes.driver.volumes.hostPath.logs_dir.mount.path=/opt/spark/logs

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
                     "--conf spark.driver.memory=2g "
                     "--conf spark.executor.memory=2g "
                     "--conf spark.kubernetes.executor.request.cores=1 "
                     "--conf spark.kubernetes.executor.limit.cores=1 "
                     "--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark "
                     "--conf spark.kubernetes.namespace=dtp "
                     "--conf spark.kubernetes.driver.volumes.hostPath.logs-dir.mount.path=/opt/spark/logs "
                     "--conf spark.kubernetes.driver.volumes.hostPath.logs-dir.options.path=/opt/spark/logs "
                     "--conf spark.kubernetes.executor.volumes.hostPath.logs-dir.mount.path=/opt/spark/logs "
                     "--conf spark.kubernetes.executor.volumes.hostPath.logs-dir.options.path=/opt/spark/logs "
                     "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar ",
    )

    example_trigger >> submit_spark_job

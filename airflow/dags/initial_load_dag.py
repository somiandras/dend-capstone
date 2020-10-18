from configparser import ConfigParser
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.aws_redshift_cluster_sensor import AwsRedshiftClusterSensor
from airflow.operators import CreateRedshiftClusterOperator

redshift_config = ConfigParser()
redshift_config.read("config/redshift.cfg")
redshift_cluster_id = redshift_config.get("CLUSTER", "CLUSTER_ID")

default_params = dict(
    owner="somiandras",
    start_date=datetime(2020, 10, 18, 0, 0, 0),
    retries=0,
)

dag = DAG(
    "initial_load_dag",
    default_args=default_params,
    schedule_interval=None,
    description="Create Redshift and EMR clusters and batch process initial dataset.",
    start_date=datetime.utcnow(),
)

start_dag_task = DummyOperator(task_id="start_dag")

create_redshift_task = CreateRedshiftClusterOperator(
    task_id="create_redshift_cluster",
    dag=dag,
)

wait_for_redshift_task = AwsRedshiftClusterSensor(
    task_id="wait_for_redshift_cluster",
    cluster_identifier=redshift_cluster_id,
    dag=dag,
)

end_dag_task = DummyOperator(task_id="end_dag")

start_dag_task >> create_redshift_task
create_redshift_task >> wait_for_redshift_task
wait_for_redshift_task >> end_dag_task

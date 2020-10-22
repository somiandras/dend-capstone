from configparser import ConfigParser
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.aws_redshift_cluster_sensor import AwsRedshiftClusterSensor
from airflow.operators import (
    CreateRedshiftClusterOperator,
    SaveRedshiftHostOperator,
)

redshift_config = ConfigParser()
redshift_config.read("config/redshift.cfg")
redshift_cluster_id = redshift_config.get("CLUSTER", "CLUSTER_ID")

default_params = dict(
    owner="somiandras",
    start_date=datetime(2020, 10, 18, 0, 0, 0),
    retries=0,
)

dag = DAG(
    dag_id="setup_redshift_dag",
    default_args=default_params,
    schedule_interval=None,
    description="Create Redshift cluster and tables.",
    start_date=datetime.utcnow(),
)

start_dag_task = DummyOperator(task_id="start_dag", dag=dag)

create_redshift_task = CreateRedshiftClusterOperator(
    task_id="create_redshift_cluster",
    dag=dag,
    config=redshift_config,
)

wait_for_redshift_task = AwsRedshiftClusterSensor(
    task_id="wait_for_redshift_cluster",
    cluster_identifier=redshift_cluster_id,
    dag=dag,
)

save_redshift_endpoint_task = SaveRedshiftHostOperator(
    task_id="save_redshift_endpoint",
    cluster_identifier=redshift_cluster_id,
    dag=dag,
    config=redshift_config,
)

create_schemas_task = PostgresOperator(
    task_id="create_schemas",
    sql=[
        "create schema if not exists stage;",
        "create schema if not exists analytics;",
    ],
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)

create_stage_tables_task = PostgresOperator(
    task_id="create_stage_tables",
    sql="sql/create_stage_tables.sql",
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)


create_analytics_tables_task = PostgresOperator(
    task_id="create_analytics_tables",
    sql="sql/create_analytics_tables.sql",
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)


end_dag_task = DummyOperator(task_id="end_dag", dag=dag)

start_dag_task >> create_redshift_task
create_redshift_task >> wait_for_redshift_task
wait_for_redshift_task >> save_redshift_endpoint_task
save_redshift_endpoint_task >> create_schemas_task

create_schemas_task >> create_stage_tables_task
create_schemas_task >> create_analytics_tables_task

create_stage_tables_task >> end_dag_task
create_analytics_tables_task >> end_dag_task
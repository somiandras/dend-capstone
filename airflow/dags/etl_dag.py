from configparser import ConfigParser
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (
    StageTripData,
    StageWeatherData,
    StageZoneData,
    CheckNullValuesOperator,
)

redshift_config = ConfigParser()
redshift_config.read("config/redshift.cfg")

default_params = dict(
    owner="somiandras",
    start_date=datetime(2020, 10, 18, 0, 0, 0),
    retries=0,
)

dag = DAG(
    dag_id="etl_dag",
    default_args=default_params,
    schedule_interval=None,
    description="Load data to Redshift from S3",
    start_date=datetime.utcnow(),
)

start_dag_task = DummyOperator(task_id="start_dag", dag=dag)

stage_trip_data_task = StageTripData(
    task_id="stage_trip_data",
    table="stage.trip",
    min_date=datetime(2020, 6, 1),
    dag=dag,
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    s3_bucket="nyc-tlc",
    s3_key="trip data",
)

stage_weather_data_task = StageWeatherData(
    task_id="stage_weather_data",
    table="stage.weather",
    min_date=datetime(2020, 6, 1),
    dag=dag,
    s3_bucket="dend-capstone-somi",
    s3_key="weather",
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
)

stage_zone_data_task = StageZoneData(
    task_id="stage_zone_data",
    table="stage.zone",
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    s3_bucket="nyc-tlc",
    s3_key="misc/taxi _zone_lookup.csv",
    dag=dag,
)

check_nulls_task = CheckNullValuesOperator(
    task_id="check_null_values",
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    query="sql/check_null.sql",
    dag=dag,
)


insert_zone_data_task = PostgresOperator(
    task_id="insert_zone_data",
    sql=[
        "truncate analytics.zone;",
        "insert into analytics.zone select * from stage.zone;",
    ],
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)

insert_weather_data_task = PostgresOperator(
    task_id="insert_weather_data",
    sql="sql/insert_weather_data.sql",
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)

insert_trip_data_task = PostgresOperator(
    task_id="insert_trip_data",
    sql="sql/insert_trip_data.sql",
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)

end_dag_task = DummyOperator(task_id="end_dag", dag=dag)


start_dag_task >> stage_trip_data_task
start_dag_task >> stage_weather_data_task
start_dag_task >> stage_zone_data_task

stage_trip_data_task >> check_nulls_task
stage_zone_data_task >> check_nulls_task
stage_weather_data_task >> check_nulls_task

check_nulls_task >> insert_zone_data_task
check_nulls_task >> insert_zone_data_task
check_nulls_task >> insert_weather_data_task

insert_zone_data_task >> insert_trip_data_task

insert_trip_data_task >> end_dag_task
insert_weather_data_task >> end_dag_task

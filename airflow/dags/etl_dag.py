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
    CheckUniqueValuesOperator,
)

redshift_config = ConfigParser()
redshift_config.read("config/redshift.cfg")

default_params = dict(
    owner="somiandras",
    retries=0,
    depends_on_past=False,
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    end_date=datetime(2020, 7, 1, 0, 0, 0),
    email_on_failure=False,
    email_on_retry=False,
)

dag = DAG(
    dag_id="etl_dag",
    description="Load data to Redshift from S3",
    default_args=default_params,
    schedule_interval="@monthly",
)

start_dag_task = DummyOperator(task_id="start_dag", dag=dag)

create_stage_tables_task = PostgresOperator(
    task_id="create_stage_tables",
    sql="sql/create_stage_tables.sql",
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)

stage_trip_data_task = StageTripData(
    task_id="stage_trip_data",
    table='stage."trip_{{ ds }}"',
    dag=dag,
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    s3_bucket="nyc-tlc",
    s3_key="trip data",
)

stage_weather_data_task = StageWeatherData(
    task_id="stage_weather_data",
    table='stage."weather_{{ ds }}"',
    dag=dag,
    s3_bucket="dend-capstone-somi",
    s3_key="weather",
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
)

stage_zone_data_task = StageZoneData(
    task_id="stage_zone_data",
    table='stage."zone_{{ ds }}"',
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    s3_bucket="nyc-tlc",
    s3_key="misc/taxi _zone_lookup.csv",
    dag=dag,
)

check_nulls_task = CheckNullValuesOperator(
    task_id="check_null_values",
    checks=[
        ("weather", ["date"]),
        (
            "trip",
            [
                "trip_distance",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "total_amount",
            ],
        ),
        ("zone", ["LocationID", "Borough", "Zone", "service_zone",]),
    ],
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)

check_unique_tasks = CheckUniqueValuesOperator(
    task_id="check_unique_values",
    checks=[
        ("zone", ["locationid"]),
        ("weather", ["station", "date"]),
        (
            "trip",
            [
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "pulocationid",
                "dolocationid",
                "total_amount",
                "payment_type",
                "trip_distance",
            ],
        ),
    ],
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)

stage_ready_task = DummyOperator(task_id="stage_ready", dag=dag)
checks_ready_task = DummyOperator(task_id="checks_ready", dag=dag)

insert_zone_data_task = PostgresOperator(
    task_id="insert_zone_data",
    sql=[
        "truncate analytics.zone;",
        'insert into analytics.zone select * from stage."zone_{{ ds }}";',
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


remove_stage_task = PostgresOperator(
    task_id="remove_stage_tables",
    sql=[
        'drop table if exists stage."trip_{{ ds }}";',
        'drop table if exists stage."weather_{{ ds }}";',
        'drop table if exists stage."zone_{{ ds }}";',
    ],
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)

end_dag_task = DummyOperator(task_id="end_dag", dag=dag)

start_dag_task >> create_stage_tables_task

create_stage_tables_task >> stage_trip_data_task
create_stage_tables_task >> stage_weather_data_task
create_stage_tables_task >> stage_zone_data_task

stage_trip_data_task >> stage_ready_task
stage_zone_data_task >> stage_ready_task
stage_weather_data_task >> stage_ready_task

stage_ready_task >> check_nulls_task
stage_ready_task >> check_unique_tasks

check_nulls_task >> checks_ready_task
check_unique_tasks >> checks_ready_task

checks_ready_task >> insert_zone_data_task
checks_ready_task >> insert_weather_data_task

insert_zone_data_task >> insert_trip_data_task

insert_trip_data_task >> remove_stage_task
insert_weather_data_task >> remove_stage_task

remove_stage_task >> end_dag_task

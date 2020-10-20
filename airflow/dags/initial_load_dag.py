from configparser import ConfigParser
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.aws_redshift_cluster_sensor import AwsRedshiftClusterSensor
from airflow.operators import (
    CreateRedshiftClusterOperator,
    SaveRedshiftHostOperator,
    StageTripData,
    StageWeatherData,
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
    dag_id="initial_load_dag",
    default_args=default_params,
    schedule_interval=None,
    description="Create Redshift and EMR clusters and batch process initial dataset.",
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

create_trip_stage_table_task = PostgresOperator(
    task_id="create_trip_stage_table",
    sql=[
        "create schema if not exists stage;",
        "drop table if exists stage.trip",
        """create table if not exists stage.trip (
            VendorID int,
            tpep_pickup_datetime varchar,
            tpep_dropoff_datetime varchar,
            passenger_count int,
            trip_distance float,
            RatecodeID int,
            store_and_fwd_flag boolean,
            PULocationID int,
            DOLocationID int,
            payment_type int,
            fare_amount float,
            extra float,
            mta_tax float,
            tip_amount float,
            tolls_amount float,
            improvement_surcharge float,
            total_amount float,
            congestion_surcharge float
        );""",
    ],
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
)

stage_trip_data_task = StageTripData(
    task_id="stage_trip_data",
    table="stage.trip",
    min_date=datetime(2020, 6, 1),
    dag=dag,
    redshift_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    s3_bucket="nyc-tlc",
    s3_key="trip data",
)

create_weather_stage_table_task = PostgresOperator(
    task_id="create_weather_stage_table",
    sql=[
        "create schema if not exists stage;",
        "drop table if exists stage.weather",
        """create table if not exists stage.weather (
            STATION varchar,
            NAME varchar,
            LATITUDE float,
            LONGITUDE float,
            ELEVATION float,
            DATE bigint,
            AWND float,
            PRCP float,
            SNOW float,
            TMAX float,
            TMIN float
        );""",
    ],
    postgres_conn_id=redshift_config.get("CLUSTER", "CLUSTER_ID"),
    dag=dag,
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

end_dag_task = DummyOperator(task_id="end_dag", dag=dag)

start_dag_task >> create_redshift_task
create_redshift_task >> wait_for_redshift_task
wait_for_redshift_task >> save_redshift_endpoint_task

save_redshift_endpoint_task >> create_trip_stage_table_task
create_trip_stage_table_task >> stage_trip_data_task
stage_trip_data_task >> end_dag_task

save_redshift_endpoint_task >> create_weather_stage_table_task
create_weather_stage_table_task >> stage_weather_data_task
stage_weather_data_task >> end_dag_task

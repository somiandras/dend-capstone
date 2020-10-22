# Udacity Data Engineering Nanodegree - Capstone Project

## Introduction

In the final project of the course my goal is to stich together most of the technologies I learned about in the different modules of the DEND into a coherent data pipeline.

For the basis of the project I chose the famous __NYC taxi trips__ dataset. This is a well known and widely used data source in data analysis and data science projects. Even if it's not a very novel dataset, from a data engineering perspective it is a great candidate for this project for several reasons:

- It is an extremely large dataset, using big data tools is essential
- Updated regularily, great for scheduled pipelines
- Can be merged with several other datasets, eg. geography, weather, etc.

## Goal of the Project

For the project I assumed that all trips included in the source data belong to a single taxi company. The goal of the project is to setup a data warehouse on AWS that can provide the management of this imaginative taxi company with high level view and the possibility to drill down and gain deeper insights about the performance of the company.

## Datasets

### 1. NYC Taxi & Limousine Commission (TLC) Trip Record Data

Source: [https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

This is the base "transactional" dataset, containing millions of individual taxi trips. The individual files are availabile in a public S3 bucket.

### 2. NYC TLC Taxi Zone Lookup Table

Source: [https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv](https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv)

This is supplementary data for identifying the zones where the individual trips started or ended. Ideal for adding a geographical dimension to the analysis.

### 3. NOAA Daily Weather Data

Source: [https://www.ncdc.noaa.gov/cdo-web/datasets#GHCND](https://www.ncdc.noaa.gov/cdo-web/datasets#GHCND)

This is a separate data source which adds weather information to the trip data. I requested the file for this project via website of NOAA.

## Technologies/Services Used

- **Storage:** AWS S3
  - As the main data already resides in S3 it made sense to put the other raw data files in S+
- **Workflow management:** Airflow
  - DAGs are perfect for reasoning about ETL processes
  - I also wanted to create a solution where bulk loads and (simulated) incremental updates can run
- **Data Warehouse:** Amazon Redshift
  - Star schema (and derived OLAP cubes) assume relational data modelling
  - Redshift provides distributed, columnar storage, which is necessary for this large amount of data
- **Supplementary tools:** PyData ecosystem (eg. Jupyter, pandas, etc.)

## Data Model

The final data model is a (small) star schema where the fact table contains trips with the detailed fare amounts, while dimension tables contain start and end locations and daily weather information.

![ERD](img/erd.png)

This schema is great for **aggregating trip counts, revenues and passenger counts** by **geographic**, **date**, and **weather** dimensions in ad-hoc queries or perodically updated OLAP cubes. I left out columns from the original data sources to streamline the data and focus on these dimensions and use cases.

### Dictionary of final tables

#### analytics.trip

- **trip_id:** unique identifier created by hashing multiple columns
- **passenger_count:** number of passengers in trip (can be missing)
- **trip_distance:** traveled distance in miles as reported by the taximeter
- **trip_duration_sec:** trip time in seconds
- **pickup_date:** date when trip started
- **pickup_location_id:** taxi zone where trip started
- **dropoff_location_id:** taxi zone where trip ended
- **rate_code:** final rate code in effect at the end of the trip
- **payment_type:** type of payment
- **fare_amount, extra, mta_tax, tip_amount, tolls_amount,  improvement_surcharge, congestion_surcharge:** components ot total amount
- **total_amount:** total amount charged to passengers
- **total_amount_check:** TRUE if total amount components add up to `total_amount`, FALSE if not

#### analytics.weather

- **date:** date of weather metrics measured at NYC Central Park weather station
- **wind:** average daily wind speed (meters per second)
- **precip:** daily precipitation in milimeters
- **snow:** daily snowfall in milimeters
- **tmax/tmin:** daily min/max temperature

#### analytics.zone

- **location_id:** id of taxizone
- **borough:** borough of NYC
- **zone:** taxi zone - smallest unit of this table
- **service_zone:** larger unit of zone structure

## Processing Steps

I manage all operations in two Airflow pipelines, from launching a Redshift cluster to creating the final data outputs. Only exception is weather data preprocessing, which I did in a separate Python script.

### 1. Preprocessing

The original csv file from NOAA contained daily observations from 134 weather stations in New York, New Jersey and Connecticut, from 2017-01-01 to 2020-07-31.

In order to simulate loading from different formats and to preprocess the datafile, I filtered the columns, split up the csv file into daily JSON files, and uploaded them to the S3 bucket in the following format: `s3://dend-capstone-somi/weather/<YEAR-MONTH>/weather-<DATE>.json` (this is done in `preprocess_weather_data.py`).

### 2. Setting up Redshift

For the sake of practicing these technologies, I created tasks in an Airflow pipeline to set up a Redshift cluster and save connection details into Airflow for later use. This way I can restart my project from scratch just by updating the config file (if necessary) and running the pipeline (if the Redshift cluster is already up and running the related tasks succeed quickly).

![Redshift DAG](img/redshift_dag.png)

### 3. ETL

I created a separate DAG for staging, transforming and loading the data when the Redshift cluster is ready.

#### 3.1 Staging

- Data from the input sources is staged in Redshift tables under `stage` schema:
  - `staging.trip`: data from the yellow cab trips data source for the current date range
  - `staging.zone`: taxi zone identification data
  - `staging.weather`: daily weather data for the current date range for all weather stations
- Tables are truncated each time the pipeline is run, making sure that no residual data from previous runs spills into the current operation.
- Staging tasks are set up in a way that they create meaningful results in scheduler DAG runs and ad-hoc (manual) runs:
  - If DAG is scheduled, import date range is between previous successful run date and current execution date
  - If DAG is run manually or there is no previous success date the import is run between the preset minimum date and current execution date

#### 3.2 Data Cleaning

[WIP]

#### 3.3 Loading to Analytics Tables

- Final tables live under the `analytics` schema
- Insertion is done via `PostgresOperators` with SQL statements in separate files
- Insert tasks only rely on data in staging tables, therefore work in batch or incremental (scheduled) DAG runs
- `analytics.zone` (dimension):
  - primary key is `location_id` which maps to `pickup_location_id` and `dropoff_location_id` on the fact table
  - it is truncated and reloaded with each import (this is a very small table)
  - this is a small table, therefore distribution style is `ALL`
- `analytics.weather` (dimnension):
  - primary key is the `date`, which maps to `pickup_date` on the fact table
  - updated incrementally, new data is inserted and existing rows are updated with the incoming values
  - `date` is the distribution key and sort key (this will require periodic repartitioning as new data comes in)
- `analytics.trip` (the fact table)
  - primary key is a hash value constructed from pickup/dropoff timestamps and location id and passenger count (this is a makeshift solution as the data source does not contain unique identifiers)
  - also updated incrementally, with new rows inserted and any existing rows completely updated with the incoming data
  `pickup_date` is the distribution key and sort key (this will require periodic repartitioning as new data comes in)

## How to Run the Project

The project can be run in Airflow by reading DAGs, plugins and configuration from `./airflow` and `./config`.

## Files in the Repository

```bash
.
├── README.md
├── airflow
│   ├── dags
│   │   ├── initial_load_dag.py
│   │   └── sql
│   │       ├── create_analytics_tables.sql
│   │       ├── create_stage_tables.sql
│   │       ├── insert_trip_data.sql
│   │       └── insert_weather_data.sql
│   └── plugins
│       ├── __init__.py
│       └── operators
│           ├── __init__.py
│           ├── create_redshift_operator.py
│           ├── save_redshift_endpoint.py
│           ├── stage_trip_data.py
│           ├── stage_weather_data.py
│           └── stage_zone.py
├── config
│   └── redshift.cfg
├── img
│   └── erd.png
├── preprocess_weather_data.py
└── requirements.txt
```

## Potential Improvements

1. **Redshift Setup:** In a real project creating Redshift cluster straight from Airflow wouldn't really make sense, but here this presented a great opportunity to practice Airflow and AWS infrastructure as code in the same exercise.
2. **Security:** When setting up the infrastructure I mostly used default values and settings. In a real life project it would be crucial to ensure the security of the setup via proper configuration of VPCs, security groups, roles and users (both on AWS and on Redshift database/schema level).
3. **Data cleaning:** Even though I did some preprocessing and quality checks on the data, I could do more on cleaning and validating the input data. In a real life project this would be crucial. Here my focus was to set up the infrastructure and pipelines, accepting some implied assumptions about the data quality.
4. **Custom operators:** Custom operators for staging the data look very similar at first sight, even though they differ in some key elements (query structure, S3 key construction, etc.). In a  larger setup it might be worth merging their functionality into a single staging operator and handle the logical differences inside this merged operator. For this project I considered as premature optimization.
5. **Deploying pipelines:** I ran Airflow on my laptop which is obviously not a viable solution in a real setup. Properly deploying Airflow in the cloud with the appropriate resources and architecture and making sure that pipelines can run smoothly could be a project in itself.
6. **OLAP Cube:** The final data lends itself nicely for creating multidimensional OLAP cubes, eg. summing up the total fare amount by date periods (weeks or months), pickup and dropoff locations and probably even weather conditions by categorizing weather variables.
7. **Utilizing Spark:** If were to process the whole NYC TLC dataset including yellow and green taxis and other trips for the whole available date range, we would need substantial resources. This operation would only be feasible if we used a Spark cluster to read data from S3 and crunch it.

## What If's

### What if the data was increased by 100x

### What if the pipelines would be run on a daily basis by 7 am every day

### What if the database needed to be accessed by 100+ people
